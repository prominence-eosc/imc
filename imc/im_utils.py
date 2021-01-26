"""Functions for preparing for Infrastructure Manager"""
from __future__ import print_function
import base64
import logging
import re

from imc import config

# Configuration
CONFIG = config.get_config()

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
                       'tenant_domain_id',
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
            elif item == 'password' and 'BEGIN PRIVATE KEY' in block[item]:
                value = block[item]
                value = value.replace('\n', '____n')
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
