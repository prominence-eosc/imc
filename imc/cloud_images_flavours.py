"""Get images & flavours available on a cloud"""

import json
import logging
import os
import re
import sys
import time
import configparser

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

from imc import config
from imc import tokens
from imc import cloud_utils
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def add_defaults(data, config):
    """
    Add any default images/flavours if they do not already exist in data retrieved from the cloud
    """
    if 'default_images' in config:
        if config['default_images']:
            for image in config['default_images']:
                if image:
                    if image not in data['images']:
                        data['images'][image] = config['default_images'][image]

    if 'default_flavours' in config:
        if config['default_flavours']:
            for flavour in config['default_flavours']:
                if flavour:
                    if flavour not in data['flavours']:
                        data['flavours'][flavour] = config['default_flavours'][flavour]

    return data

def update_images(db, cloud, identity, images):
    """
    Update images in the database
    """
    for image_name in images:
        image = images[image_name]
        logger.info('Setting image in DB: name=%s, im=%s', image_name, image['im_name'])
        db.set_image(identity,
                     cloud,
                     image_name,
                     image['im_name'],
                     image['type'],
                     image['architecture'],
                     image['distribution'],
                     image['version'])

def delete_images(db, cloud, identity, old, new):
    """
    Delete images which no longer exist from the database
    """
    for image_old in old:
        name_old = old[image_old]['name']
        found = False
        for image_new in new:
            name_new = new[image_new]['name']
            if name_old == name_new:
                return True
        if not found:
            db.delete_image(identity, cloud, name_old)

def update_flavours(db, cloud, identity, flavours):
    """
    Update flavours in database
    """
    for flavour_name in flavours:
        flavour = flavours[flavour_name]
        db.set_flavour(identity,
                       cloud,
                       flavour['name'],
                       flavour['cpus'],
                       flavour['memory'],
                       flavour['disk'])

def is_power2(num):
    """
    Check if a number is a power of 2
    """
    return num != 0 and ((num & (num - 1)) == 0)

def memory_convert(value):
    """
    Different OpenStack admins define memory units differently, try to 
    handle this
    """
    m1 = int(value/1000.0)
    m2 = int(value/1024.0)
    m = m2
    if is_power2(m1):
        m = m1
    if is_power2(m2):
        m = m2
    return m

def compare_dicts(cloud1, cloud2):
    """
    Compare the dicts containing cloud images or flavours
    """
    if len(cloud1) != len(cloud2):
        return False

    for item in cloud1:
        if item in cloud2:
            if cloud1[item] != cloud2[item]:
                return False
        else:
            return False
    return True

def generate_images_and_flavours(config, cloud, token):
    """
    Create a list of images and flavours available on the specified cloud
    """
    output = {}
    output['images'] = None
    output['flavours'] = None

    # Connect to the cloud
    conn = cloud_utils.connect_to_cloud(cloud, config, token)
    if not conn:
        return output

    # List images
    try:
        images = conn.list_images()
    except Exception as ex:
        logger.critical('Unable to get list of images from cloud %s due to "%s"', cloud, ex)
        return output

    output_images = {}
    for image in images:
        for image_t in config['image_templates']:
            if image_t in image.name:
                image_identifier = image.name
                if config['credentials']['type'] == 'OpenStack':
                    image_identifier = image.id

                data = config['image_templates'][image_t]
                data['im_name'] = '%s/%s' % (config['image_prefix'], image_identifier)
                data['name'] = image.name
                output_images[image.name] = data

    output['images'] = output_images
    logger.info('Got %d images from cloud %s', len(output['images']), cloud)

    # List flavours
    try:
        flavours = conn.list_sizes()
    except Exception as ex:
        logger.critical('Unable to get list of flavours from cloud %s due to "%s"', cloud, ex)
        return output

    output_flavours = {}
    for flavour in flavours:
        match_obj_name = False
        use = True
        if 'blacklist' in config['flavour_filters']:
            match_obj_name = re.match(r'%s' % config['flavour_filters']['blacklist'], flavour.name)
            use = False

        if not match_obj_name or use:
            try:
                data = {"name":flavour.name,
                        "cpus":flavour.vcpus,
                        "memory":memory_convert(flavour.ram),
                        "disk":flavour.disk}
                output_flavours[flavour.name] = data
            except:
                pass

    output['flavours'] = output_flavours
    logger.info('Got %d flavours from cloud %s', len(output['flavours']), cloud)

    return output

def update(db, identity, config):
    """
    Update cloud images & flavours if necessary
    """
    for cloud in config:
        name = cloud['name']

        if cloud['type'] != 'cloud':
            continue

        logger.info('Checking if we need to update cloud %s details', name)

        # Get a token if necessary
        logger.info('Getting a new token if necessary')
        token = tokens.get_token(name, identity, db, config)

        # Get new images & flavours
        logger.info('Getting list of new images and flavours')
        try:
            new_data = generate_images_and_flavours(cloud, name, token)
        except Exception as err:
            logger.critical('Got exception generating images and flavours: %s', err)
            new_data = {'images':{}, 'flavours':{}}

        logger.info('Adding default images & flavours')
        new_data = add_defaults(new_data, cloud)

        # Check if need to continue with this cloud
        if not new_data['images'] and not new_data['flavours']:
            logger.info('Not continuing with considering updating details for cloud %s as there is no data', name)
            continue

        # Check if we need to update
        requires_update = False
        last_update = db.get_cloud_updated_images(name, identity)
        if time.time() - last_update > int(CONFIG.get('updates', 'vms')):
            logger.info('Images and flavours for cloud %s have not been updated recently', name)
            requires_update = True

        # Get existing images & flavours
        images_old = db.get_images(identity, name)
        flavours_old = db.get_all_flavours(identity, name)

        updated = False

        # Update cloud VM images if necessary
        if (not images_old or requires_update or not compare_dicts(images_old, new_data['images'])) and new_data['images']:
            if not compare_dicts(images_old, new_data['images']):
                logger.info('Updating images in DB for cloud %s', name)
                delete_images(db, name, identity, images_old, new_data['images'])
                update_images(db, name, identity, new_data['images'])
                updated = True
            else:
                logger.info('Images for cloud %s have not changed, not updating', name)
 
        # Update cloud VM flavours if necessary
        if (not flavours_old or requires_update or not compare_dicts(flavours_old, new_data['flavours'])) and new_data['flavours']:
            if not compare_dicts(flavours_old, new_data['flavours']):
                logger.info('Updating flavours in DB for cloud %s', name)
                update_flavours(db, name, identity, new_data['flavours'])
                updated = True
            else:
                logger.info('Flavours for cloud %s have not changed, not updating', name)

        if updated:
            db.set_cloud_updated_images(name, identity)

    return True
