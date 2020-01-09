""" Worker pool """

from __future__ import print_function
from concurrent.futures import ProcessPoolExecutor
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

def infrastructure_deploy(input_json, unique_id, identity):
    """
    Deploy infrastructure given a JSON specification and id
    """
    print('in infrastructure_deploy')
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': unique_id})
    logger.info('Inside infrastructure_deploy for infra', unique_id)
    try:
        imc.auto_deploy(input_json, unique_id, identity)
    except Exception as error:
        logger.critical('Exception deploying infrastructure: "%s"', error)
    return

def infrastructure_delete(unique_id):
    """
    Delete the infrastructure with the specified id
    """
    print('in infrastructure_delete')
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': unique_id})
    logger.info('Inside infrastructure_delete for infra', unique_id)
    try:
        imc.delete(unique_id)
    except Exception as error:
        logger.critical('Exception deleting infrastructure: "%s"', error)
    return

def find_new_infra_for_creation(executor, db):
    """
    Find infrastructure to be deployed
    """
    infras = db.deployment_get_infra_in_state_cloud('accepted', None)
    for infra in infras:
        logger.info('Running deploying for infra %s', infra['id'])
        (description, identity) = db.deployment_get_json(infra['id'])
        executor.submit(infrastructure_deploy, description, infra['id'], identity)

def find_new_infra_for_deletion(executor, db):
    """
    Find infrastructure to be destroyed
    """
    infras = db.deployment_get_infra_in_state_cloud('deletion-requested', None)
    for infra in infras:
        logger.info('Running deletion for infra %s', infra['id'])
        executor.submit(infrastructure_delete, infra['id'])

if __name__ == "__main__":
    # Process pool for handling deployments & deletions
    executor = ProcessPoolExecutor(int(CONFIG.get('pool', 'size')))

    # Main loop
    while True:
        db = database.get_db()
        if db.connect():
            logger.info('Checking for new infrastructures to deploy...')
            find_new_infra_for_creation(executor, db)
            logger.info('Checking for new infrastructures to delete...')
            find_new_infra_for_deletion(executor, db)
        db.close()
        time.sleep(30)

