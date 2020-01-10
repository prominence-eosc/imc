""" Worker pool """

from __future__ import print_function
import configparser
import logging
from logging.handlers import RotatingFileHandler
import re
import os
import sys
import time

import database
import imc
import imclient
import opaclient
import tokens
import utilities
import logger as custom_logger

# Configuration
CONFIG = configparser.ConfigParser()
if 'PROMINENCE_IMC_CONFIG_DIR' in os.environ:
    CONFIG.read('%s/imc.ini' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
else:
    print('ERROR: Environment variable PROMINENCE_IMC_CONFIG_DIR has not been defined')
    exit(1)

# Logging
handler = RotatingFileHandler(CONFIG.get('logs', 'filename'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('deployment-worker')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    infra_id = sys.argv[1]
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': infra_id})
    logger.info('Starting deployment of infrastructure')

    db = database.get_db()
    if db.connect():
        try:
            imc.auto_deploy(infra_id)
        except Exception as error:
            logger.critical('Exception deploying infrastructure: "%s"', error)
    db.close()

    logger.info('Completed deploying infrastructure')

