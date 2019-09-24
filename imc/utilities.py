"""Miscellaneous functions"""

import glob
import json
import logging
import sys

import opaclient

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

def create_basic_radl(radl):
    """
    Generates new RADL with all configure and contextualize blocks removed.
    """
    ignore = False
    skip_next_line = False
    radl_new = ''

    for line in radl.split('\n'):
        if line.startswith('configure ') or line.startswith('contextualize'):
            ignore = True

        if not ignore and not skip_next_line:
            radl_new += '%s\n' % line

        if skip_next_line:
            skip_next_line = False

        if line.startswith('@end') and ignore:
            ignore = False
            skip_next_line = True

    return radl_new

def create_im_line(name, block, token):
    """
    Create a line for an IM auth file
    """
    valid_im_fields = ['type',
                       'username',
                       'password',
                       'tenant',
                       'host',
                       'proxy',
                       'project',
                       'public_key',
                       'private_key',
                       'subscription_id',
                       'domain',
                       'auth_version',
                       'api_version',
                       'base_url',
                       'network_url',
                       'image_url',
                       'volume_url',
                       'service_region',
                       'service_name',
                       'auth_token']

    im_auth_line = 'id = %s; ' % name
    for item in block:
        if item in valid_im_fields:
            if item == 'password' and token is not None:
                value = token
            else:
                value = block[item]
            line = '%s = %s; ' % (item, value)
            im_auth_line += line
    return im_auth_line

def create_im_auth(cloud, token, config):
    """
    Create the auth file required for requests to IM, inserting tokens as necessary
    """
    data = {}
    for cloud_info in config:
        if cloud_info['name'] == cloud:
            data = cloud_info

    if not data:
        logger.critical('Required cloud (%s) not in cloud config', cloud)
        return None

    if 'credentials' not in data:
        logger.critical('Invalid JSON config file for cloud %s: credentials missing', cloud)
        return None

    if not cloud:
        return '%s\\n' % create_im_line('IM', data['credentials']['IM'], None)
    elif cloud not in data['credentials']:
        logger.critical('Credentials for the cloud %s are not in the JSON config file', cloud)
        return None

    return '%s\\n%s\\n' % (create_im_line('IM', data['credentials']['IM'], None),
                           create_im_line(cloud, data['credentials'][cloud], token))

def create_clouds_list(path):
    """
    Generate full list of clouds
    """
    clouds = []

    cloud_files = glob.glob('%s/*.json' % path)
    for cloud_file in cloud_files:
        data = {}
        try:
            with open(cloud_file) as fd:
                data = json.load(fd)
        except Exception as err:
            pass

        if data:
            clouds.append(data)

    return clouds

def create_clouds_for_opa(path):
    """
    Generate list of clouds and properties for Open Policy Agent
    """
    clouds = {}
    cloud_names = []

    cloud_files = glob.glob('%s/*.json' % path)
    for cloud_file in cloud_files:
        data = {}
        try:
            with open(cloud_file) as fd:
                data = json.load(fd)
        except Exception as err:
            pass

        if data:
            cloud = {}
            name = data['name']
            cloud['name'] = name
            cloud['images'] = data['default_images']
            if 'region' in data:
                cloud['region'] = data['region']
            if 'tags' in data:
                cloud['tags'] = data['tags']
            if 'quotas' in data:
                cloud['quotas'] = data['quotas']
            if 'network' in data:
                cloud['network'] = data['network']
            if 'supported_groups' in data:
                cloud['supported_groups'] = data['supported_groups']
            cloud['flavours'] = data['default_flavours']
            clouds[name] = cloud
            cloud_names.append(name)

    logger.info('Found clouds: %s', ','.join(cloud_names))

    return clouds

def compare_dicts(cloud1, cloud2, ignores):
    """
    Compare the dicts containing cloud info
    """
    if not cloud1 or not cloud2:
        return False

    if len(cloud1) != len(cloud2):
        return False

    for item in cloud1:
        if item in cloud2:
            if cloud1[item] != cloud2[item] and item not in ignores:
                return False
        elif item not in ignores:
            return False
    return True

def update_clouds(opa_client, path):
    """
    Update clouds
    """
    new_cloud_info = create_clouds_for_opa(path)
    if not new_cloud_info:
        logger.info('No cloud info')
        return

    for new_cloud in new_cloud_info:
        name = new_cloud_info[new_cloud]['name']
        logger.info('Checking cloud %s', name)
        old_cloud = opa_client.get_cloud(name)
        if not compare_dicts(new_cloud_info[new_cloud], old_cloud, ['images', 'flavours']):
            logger.info('Updating cloud %s', name)
            opa_client.set_cloud(name, new_cloud_info[new_cloud])

    logger.info('Completed updating static cloud info')

