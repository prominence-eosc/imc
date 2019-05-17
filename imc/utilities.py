"""Miscellaneous functions"""

import json
import logging
import sys

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

def create_im_auth(cloud, token, config_file):
    """
    Create the auth file required for requests to IM, inserting tokens as necessary
    """
    try:
        with open(config_file) as file:
            data = json.load(file)
    except Exception as ex:
        logger.critical('Unable to load JSON config file due to: %s', ex)
        return None

    return '%s\\n%s\\n' % (create_im_line('IM', data['credentials']['IM'], None),
                           create_im_line(cloud, data['credentials'][cloud], token))
