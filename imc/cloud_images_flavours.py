import json
import logging
import os
import re
import sys
import time

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

import opaclient
import tokens
import utilities

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

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
    if config['credentials']['type'] == 'OpenStack':
        details = {}
        if config['credentials']['auth_version'] == '3.x_password':
            details['ex_force_auth_url'] = config['credentials']['host']
            if 'auth_version' in config['credentials']:
                details['ex_force_auth_version'] = config['credentials']['auth_version']
            if 'tenant' in config['credentials']:
                details['ex_tenant_name'] = config['credentials']['tenant']
            if 'domain' in config['credentials']:
                details['ex_domain_name'] = config['credentials']['domain']
            if 'service_region' in config['credentials']:
                details['ex_force_service_region'] = config['credentials']['service_region']

            provider = get_driver(Provider.OPENSTACK)
            try:
                conn = provider(config['credentials']['username'],
                                config['credentials']['password'],
                                **details)
            except Exception as ex:
                logger.critical('Unable to connect to cloud %s due to "%s"', cloud, ex)
                return None
        elif config['credentials']['auth_version'] == '3.x_oidc_access_token':
            details['ex_force_auth_url'] = config['credentials']['host']
            if 'auth_version' in config['credentials']:
                details['ex_force_auth_version'] = config['credentials']['auth_version']
            if 'tenant' in config['credentials']:
                details['ex_tenant_name'] = config['credentials']['tenant']
            if 'domain' in config['credentials']:
                details['ex_domain_name'] = config['credentials']['domain']
            if 'service_region' in config['credentials']:
                details['ex_force_service_region'] = config['credentials']['service_region']

            provider = get_driver(Provider.OPENSTACK)
            try:
                conn = provider(config['credentials']['username'],
                                token,
                                **details)
            except Exception as ex:
                logger.critical('Unable to connect to cloud %s due to "%s"', cloud, ex)
                return None
        else:
            return None
    else:
        return None

    output = {}

    try:
        images = conn.list_images()
    except Exception as ex:
        logger.critical('Unable to get list of images from cloud %s due to "%s"', cloud, ex)
        return None

    output_images = {}
    for image in images:
        if image.name in config['image_templates']:
            data = config['image_templates'][image.name]
            data['name'] = '%s/%s' % (config['image_prefix'], image.id)
            output_images[image.name] = data

    output['images'] = output_images

    try:
        flavours = conn.list_sizes()
    except Exception as ex:
        logger.critical('Unable to get list of flavours from cloud %s due to "%s"', cloud, ex)
        return None

    output_flavours = {}
    for flavour in flavours:
        match_obj_name = False
        use = True
        if 'blacklist' in config['flavour_filters']:
            match_obj_name = re.match(r'%s' % config['flavour_filters']['blacklist'], flavour.name)
            use = False

        if not match_obj_name or use:
            data = {"name":flavour.name,
                    "cores":flavour.vcpus,
                    "memory":memory_convert(flavour.ram),
                    "disk":flavour.disk}
            output_flavours[flavour.name] = data

    output['flavours'] = output_flavours

    return output

def update_cloud_details(requirements, db, opa_client, config):
    """
    Update cloud images & flavours if necessary
    """
    for cloud in config:
        name = cloud['name']
        # Check if we need to consider this cloud at all
        if 'sites' in requirements:
            if name not in requirements['sites']:
                continue

        if cloud['credentials']['type'] != "InfrastructureManager":
            logger.info('Checking if we need to update cloud %s details', name)

            # Get a token if necessary
            logger.info('Getting a new token if necessary')
            token = tokens.get_token(name, db, config)

            # Get new images * flavours
            logger.info('Getting list of new images and flavours')
            new_data = generate_images_and_flavours(cloud, name, token)

            # Check if need to continue with this cloud
            if new_data is None:
                logger.info('Not continuing with considering updating details for cloud', name)
                continue
    
            # Get old images & flavours
            images_old = opa_client.get_images(name)
            flavours_old = opa_client.get_flavours(name)

            # Check if the cloud hasn't been updated recently
            update_time = opa_client.get_cloud_update_time(name)
            requires_update = False
            if time.time() - update_time > 1800:
                logger.info('Images and flavours for cloud %s have not been updated recently', name)
                requires_update = True

            # Update cloud VM images if necessary
            if (images_old is None or requires_update or not compare_dicts(images_old, new_data['images'])) and new_data is not None:
                if not compare_dicts(images_old, new_data['images']):
                    logger.info('Updating images for cloud %s', name)
                    opa_client.set_images(name, new_data['images'])
                else:
                    logger.info('Images for cloud %s have not changed, not updating', name)
                opa_client.set_update_time(name)
 
            # Update cloud VM flavours if necessary
            if (flavours_old is None or requires_update or not compare_dicts(flavours_old, new_data['flavours'])) and new_data is not None:
                if not compare_dicts(flavours_old, new_data['flavours']):
                    logger.info('Updating flavours for cloud %s', name)
                    opa_client.set_flavours(name, new_data['flavours'])
                else:
                    logger.info('Flavours for cloud %s have not changed, not updating', name)
                opa_client.set_update_time(name)

    return True
