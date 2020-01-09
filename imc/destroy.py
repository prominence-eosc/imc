"""Destroy the specified IM infrastructure, with retries"""

from __future__ import print_function
import os
import sys
import time
import logging
import configparser

# Configuration
CONFIG = configparser.ConfigParser()
if 'PROMINENCE_IMC_CONFIG_DIR' in os.environ:
    CONFIG.read('%s/imc.ini' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
else:
    print('ERROR: Environment variable PROMINENCE_IMC_CONFIG_DIR has not been defined')
    exit(1)

# Logging
logger = logging.getLogger(__name__)

def destroy(client, infrastructure_id):
    """
    Destroy the specified infrastructure, including retries since clouds can be unreliable
    """
    count = 0
    delay_factor = float(CONFIG.get('deletion', 'factor'))
    delay = delay_factor
    destroyed = False
    while not destroyed and count < int(CONFIG.get('deletion', 'retries')):
        (return_code, msg) = client.destroy(infrastructure_id, int(CONFIG.get('timeouts', 'deletion')))
        if return_code == 0:
            destroyed = True
        count += 1
        delay = delay*delay_factor
        time.sleep(int(count + delay))

    if destroyed:
        logger.info('Destroyed infrastructure with IM id %s', infrastructure_id)
    else:
        logger.critical('Unable to destroy infrastructure with IM id %s due to: "%s"', infrastructure_id, msg)

    return destroyed
