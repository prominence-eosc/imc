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
    if config['credentials'][cloud]['type'] == 'OpenStack':
        details = {}
        if config['credentials'][cloud]['auth_version'] == '3.x_password':
            details['ex_force_auth_url'] = config['credentials'][cloud]['host']
            if 'auth_version' in config['credentials'][cloud]:
                details['ex_force_auth_version'] = config['credentials'][cloud]['auth_version']
            if 'tenant' in config['credentials'][cloud]:
                details['ex_tenant_name'] = config['credentials'][cloud]['tenant']
            if 'domain' in config['credentials'][cloud]:
                details['ex_domain_name'] = config['credentials'][cloud]['domain']
            if 'service_region' in config['credentials'][cloud]:
                details['ex_force_service_region'] = config['credentials'][cloud]['service_region']

            provider = get_driver(Provider.OPENSTACK)
            try:
                conn = provider(config['credentials'][cloud]['username'],
                                config['credentials'][cloud]['password'],
                                **details)
            except Exception as ex:
                logger.critical('Unable to connect to cloud %s due to "%s"', cloud, ex)
                return None
        elif config['credentials'][cloud]['auth_version'] == '3.x_oidc_access_token':
            details['ex_force_auth_url'] = config['credentials'][cloud]['host']
            if 'auth_version' in config['credentials'][cloud]:
                details['ex_force_auth_version'] = config['credentials'][cloud]['auth_version']
            if 'tenant' in config['credentials'][cloud]:
                details['ex_tenant_name'] = config['credentials'][cloud]['tenant']
            if 'domain' in config['credentials'][cloud]:
                details['ex_domain_name'] = config['credentials'][cloud]['domain']
            if 'service_region' in config['credentials'][cloud]:
                details['ex_force_service_region'] = config['credentials'][cloud]['service_region']

            provider = get_driver(Provider.OPENSTACK)
            try:
                conn = provider(config['credentials'][cloud]['username'],
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
        if image.name in config['images'][cloud]['images']:
            data = config['images'][cloud]['images'][image.name]
            data['name'] = '%s/%s' % (config['images'][cloud]['imagePrefix'], image.id)
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
        if 'blacklist' in config['flavours'][cloud]:
            match_obj_name = re.match(r'%s' % config['flavours'][cloud]['blacklist'], flavour.name)
            use = False

        if not match_obj_name or use:
            data = {"name":flavour.name,
                    "cores":flavour.vcpus,
                    "memory":memory_convert(flavour.ram),
                    "disk":flavour.disk}
            output_flavours[flavour.name] = data

    output['flavours'] = output_flavours

    return output

def update_cloud_details(requirements, db, opa_client, config_file):
    """
    Update cloud images & flavours if necessary
    """
    try:
        with open(config_file) as file:
            config = json.load(file)
    except Exception as ex:
        logger.critical('Unable to open JSON config file due to: %s', ex)
        return False

    for cloud in config['credentials']:
        # Check if we need to consider this cloud at all
        if 'sites' in requirements:
            if cloud not in requirements['sites']:
                continue

        if config['credentials'][cloud]['type'] != "InfrastructureManager":
            logger.info('Checking if we need to update cloud %s details', cloud)

            # Get a token if necessary
            logger.info('Getting a new token if necessary')
            token = tokens.get_token(cloud, db, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])

            # Get new images * flavours
            logger.info('Getting list of new images and flavours')
            new_data = generate_images_and_flavours(config, cloud, token)

            # Check if need to continue with this cloud
            if new_data is None:
                logger.info('Not continuing with considering updating details for cloud', cloud)
                continue
    
            # Get old images & flavours
            images_old = opa_client.get_images(cloud)
            flavours_old = opa_client.get_flavours(cloud)

            # Check if the cloud hasn't been updated recently
            update_time = opa_client.get_cloud_update_time(cloud)
            requires_update = False
            if time.time() - update_time > 1800:
                logger.info('Images and flavours for cloud %s have not been updated recently', cloud)
                requires_update = True

            # Update cloud VM images if necessary
            if (images_old is None or requires_update or not compare_dicts(images_old, new_data['images'])) and new_data is not None:
                if not compare_dicts(images_old, new_data['images']):
                    logger.info('Updating images for cloud %s', cloud)
                    opa_client.set_images(cloud, new_data['images'])
                else:
                    logger.info('Images for cloud %s have not changed, not updating', cloud)
                opa_client.set_update_time(cloud)
 
            # Update cloud VM flavours if necessary
            if (flavours_old is None or requires_update or not compare_dicts(flavours_old, new_data['flavours'])) and new_data is not None:
                if not compare_dicts(flavours_old, new_data['flavours']):
                    logger.info('Updating flavours for cloud %s', cloud)
                    opa_client.set_flavours(cloud, new_data['flavours'])
                else:
                    logger.info('Flavours for cloud %s have not changed, not updating', cloud)
                opa_client.set_update_time(cloud)

    return True
