"""Miscellaneous functions"""

from __future__ import print_function
import base64
import glob
import json
import logging
import re
import os
import configparser

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
import timeout_decorator

from imc import config
from imc import opaclient
from imc import tokens

# Configuration
CONFIG = config.get_config()
CLOUD_TIMEOUT = int(CONFIG.get('timeouts', 'cloud'))

# Logging
logger = logging.getLogger(__name__)

def set_availability_zone(radl, zone):
    """
    Modify RADL to set availability zone
    """
    radl_new = ''

    for line in radl.split('\n'):
        if line.startswith('system'):
            line = line + "\navailability_zone = '%s' and" % zone
        radl_new = radl_new + line + '\n'

    return radl_new

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
            if item == 'password' and token:
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
    # Create IM credentials
    credentials_im = {}
    credentials_im['username'] = CONFIG.get('im', 'username')
    credentials_im['password'] = CONFIG.get('im', 'password')
    credentials_im['type'] = 'InfrastructureManager'

    # Return only IM credentials if needed
    if not cloud:
        return '%s\\n' % create_im_line('IM', credentials_im, None)

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

    return '%s\\n%s\\n' % (create_im_line('IM', credentials_im, None),
                           create_im_line(cloud, data['credentials'], token))

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

def create_resources_for_opa(path):
    """
    Generate list of resources and properties for Open Policy Agent
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
            if 'availability_zones' in data:
                cloud['availability_zones'] = data['availability_zones']
            if 'enabled' in data:
                cloud['enabled'] = data['enabled']
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

    for item in cloud1:
        if item in cloud2:
            if cloud1[item] != cloud2[item] and item not in ignores:
                return False
        elif item not in ignores:
            return False
    return True

def update_resources(opa_client, path):
    """
    Update clouds
    """
    new_cloud_info = create_resources_for_opa(path)
    if not new_cloud_info:
        logger.info('No resources info')
        return

    # Update existing clouds or add new clouds
    new_cloud_names = []
    for new_cloud in new_cloud_info:
        name = new_cloud_info[new_cloud]['name']
        new_cloud_names.append(name)
        logger.info('Checking cloud %s', name)

        try:
            old_cloud = opa_client.get_cloud(name)
        except Exception as err:
            logger.critical('Unable to get cloud info due to %s:', err)
            return

        if not compare_dicts(new_cloud_info[new_cloud], old_cloud, ['images', 'flavours', 'updated']):
            logger.info('Updating cloud %s', name)
            opa_client.set_cloud(name, new_cloud_info[new_cloud])

    # Remove clouds if necessary
    existing_clouds = opa_client.get_all_clouds()
    for cloud in existing_clouds:
        if cloud not in new_cloud_names:
            logger.info('Removing cloud %s from Open Policy Agent', cloud)
            opa_client.delete_cloud(cloud)

    logger.info('Completed updating static cloud info')

def connect_to_cloud(cloud, config, token):
    """
    Connect to a cloud using LibCloud
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
            if 'project_domain_id' in config['credentials']:
                details['ex_tenant_domain_id'] = config['credentials']['project_domain_id']

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

    return conn

def update_clouds_status(opa_client, db, identity, config):
    """
    Update status of each cloud
    """
    for cloud_info in config:
        name = cloud_info['name']
        logger.info('Checking cloud %s', name)

        # Get a token if necessary
        token = tokens.get_token(name, identity, db, config)

        try:
            status = check_cloud(name, cloud_info, token)
        except timeout_decorator.timeout_decorator.TimeoutError:
            logger.info('Setting status of cloud %s to down due to timeout', name)
            opa_client.set_status(name, 'down')
        else:
            if not status:
                logger.info('Setting status of cloud %s to down', name)
                opa_client.set_status(name, 'down')

@timeout_decorator.timeout(CLOUD_TIMEOUT)
def check_cloud(cloud, config, token):
    """
    Check if a cloud is functional by listing VMs
    """
    # Connect to the cloud
    conn = connect_to_cloud(cloud, config, token)
    if not conn:
        return False

    # List VMs
    try:
        nodes = conn.list_nodes()
    except Exception as ex:
        logger.warn('Unable to list VMs on cloud %s due to %s', cloud, ex)
        return False

    return True

def valid_uuid(uuid):
    """
    Check if the given string is a valid uuid
    """
    regex = re.compile('^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z', re.I)
    match = regex.match(uuid)
    return bool(match)

def get_radl(description):
    """
    Extract & decode RADL from input JSON description
    """
    if 'radl' in description:
        try:
            radl = base64.b64decode(description['radl']).decode('utf8')
        except Exception as err:
            logger.warning('Invalid RADL provided: cannot be decoded')
            return None

        return radl
    return None

def get_reqs_and_prefs(description):
    """
    Extract the requirements & preferences from the input JSON description
    """
    if 'preferences' in description:
        preferences_new = {}
        # Generate list of weighted regions if necessary
        if 'regions' in description['preferences']:
            preferences_new['regions'] = {}
            for i in range(0, len(description['preferences']['regions'])):
                preferences_new['regions'][description['preferences']['regions'][i]] = len(description['preferences']['regions']) - i
        # Generate list of weighted sites if necessary
        if 'sites' in description['preferences']:
            preferences_new['sites'] = {}
            for i in range(0, len(description['preferences']['sites'])):
                preferences_new['sites'][description['preferences']['sites'][i]] = len(description['preferences']['sites']) - i
        description['preferences'] = preferences_new
    else:
        preferences = {}

    if 'requirements' in description:
        requirements = description['requirements']
        preferences = description['preferences']
    else:
        requirements = {}
    
    return (requirements, preferences)

def get_num_instances(radl):
    """
    Count the number of VM instances required
    """
    instances = 0
    for line in radl.split('\n'):
        m = re.search(r'deploy.*\s(\d+)', line)
        if m:
            instances += int(m.group(1))
    return instances
