""" Worker pool """

from __future__ import print_function
import configparser
import logging
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

if __name__ == "__main__":
    infra_id = sys.argv[1]
    logging.basicConfig(filename=CONFIG.get('logs', 'filename').replace('.log', '-deployer-%s.log' % infra_id),
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')
    
    logging.info('Starting deployment of infrastructure')

    db = database.get_db()
    if db.connect():
        imc.auto_deploy(infra_id)
    db.close()

    logging.info('Completed deploying infrastructure')

