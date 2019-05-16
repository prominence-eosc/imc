import json
import logging
import re
import sys
import time

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

import opaclient

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

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

def generate_images_and_flavours(config, cloud):
    """
    Create a list of images and flavours available on the specified cloud
    """
    if config['credentials'][cloud]['type'] == 'OpenStack':
        auth_version = config['credentials'][cloud]['auth_version']
        if auth_version == '3.x_password':
            username = config['credentials'][cloud]['username']
            password = config['credentials'][cloud]['password']
            auth_url = config['credentials'][cloud]['host']
            tenant_name = config['credentials'][cloud]['tenant']

            provider = get_driver(Provider.OPENSTACK)
            try:
                conn = provider(username,
                                password,
                                ex_force_auth_url=auth_url,
                                ex_force_auth_version=auth_version,
                                ex_tenant_name=tenant_name)
            except Exception as ex:
                return None

    output = {}

    output_images = {}
    images = conn.list_images()
    for image in images:
        if image.name in config['images'][cloud]['images']:
            data = config['images'][cloud]['images'][image.name]
            data['name'] = '%s/%s' % (config['images'][cloud]['imagePrefix'], image.id)
            output_images[image.name] = data

    output['images'] = output_images

    flavours = conn.list_sizes()
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
                    "memory":int(flavour.ram/1024),
                    "disk":flavour.disk}
            output_flavours[flavour.name] = data

    output['flavours'] = output_flavours

    return output

def update_cloud_details(opa_client, config_file):
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
        if config['credentials'][cloud]['type'] != "InfrastructureManager" and cloud == 'OpenStack-STFC':
            logger.info('Checking if we need to update cloud %s details', cloud)
            new_data = generate_images_and_flavours(config, cloud)

            images_old = opa_client.get_images(cloud)
            flavours_old = opa_client.get_flavours(cloud)

            update_time = opa_client.get_update_time(cloud)
            requires_update = False
            if time.time() - update_time > 3600:
                logger.info('Cloud %s has not been updated recently', cloud)
                requires_update = True

            if images_old is None or requires_update or not compare_dicts(images_old, new_data['images']):
                logger.info('Updating images for cloud %s', cloud)
                opa_client.set_images(cloud, new_data['images'])
                opa_client.set_update_time(cloud)

            if flavours_old is None or requires_update or not compare_dicts(flavours_old, new_data['flavours']):
                logger.info('Updating flavours for cloud %s', cloud)
                opa_client.set_flavours(cloud, new_data['flavours'])
                opa_client.set_update_time(cloud)

    return True
