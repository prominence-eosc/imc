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

def create_im_auth(cloud, token, config_file):
    """
    Create the "auth file" required for requests to IM, inserting tokens as necessary
    """
    try:
        with open(config_file) as file:
            data = json.load(file)
    except Exception as ex:
        logger.critical('Unable to load JSON config file due to: %s', ex)
        return None

    info1 = {}
    info2 = {}
    if token is not None:
        info1['token'] = token
    else:
        info2['token'] = 'not-required'

    im_auth_file = ''
    for item in data['im']['auth']:
        line = '%s\\n' % data['im']['auth'][item]
        if item == cloud and token is not None:
            line = line % info1
        else:
            line = line % info2
        line = line.replace('\n', '____n')
        im_auth_file += line

    return im_auth_file
