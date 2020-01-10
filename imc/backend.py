""" Worker pool """

from __future__ import print_function
import configparser
import logging
from logging.handlers import RotatingFileHandler
import re
import os
import sys
import subprocess
import time

import database
import imc
import imclient
import opaclient
import tokens
import utilities

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
logger = logging.getLogger('backend')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Initialize DB if necessary
dbi = database.get_db()
dbi.init()

def find_new_infra_for_creation(db):
    """
    Find infrastructure to be deployed
    """
    infras = db.deployment_get_infra_in_state_cloud('accepted', None)
    for infra in infras:
        logger.info('Running deploying for infra %s', infra['id'])
        subprocess.Popen(['python3', '/imc/deployment-worker.py', infra['id']])

def find_new_infra_for_deletion(db):
    """
    Find infrastructure to be destroyed
    """
    infras = db.deployment_get_infra_in_state_cloud('deletion-requested', None)
    for infra in infras:
        logger.info('Running deletion for infra %s', infra['id'])
        subprocess.Popen(['python3', '/imc/deletion-worker.py', infra['id']])

if __name__ == "__main__":
    while True:
        db = database.get_db()
        if db.connect():
            logger.info('Checking for new infrastructures to deploy...')
            find_new_infra_for_creation(db)
            logger.info('Checking for new infrastructures to delete...')
            find_new_infra_for_deletion(db)
        db.close()
        time.sleep(30)

