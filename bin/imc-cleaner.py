#!/usr/bin/env python
"""Periodic cleaning of infrastructure and the database"""

import configparser
import logging
from logging.handlers import RotatingFileHandler
import re
import os
import sys
import time

from imc import config
from imc import cloud_utils
from imc import database
from imc import destroy
from imc import imclient
from imc import tokens
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
handler = RotatingFileHandler(filename=CONFIG.get('logs', 'filename').replace('.log', '-cleaner.log'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('imc')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def retry_incomplete_deletions(db, state):
    """
    Retry failed deletions
    """
    infras = db.deployment_get_infra_in_state_cloud(state) 
    logger.info('Found %d infrastructures in state %s', len(infras), state)
    for infra in infras:
        if time.time() - infra['updated'] > int(CONFIG.get('cleanup', 'retry_failed_deletes_after')) or 1 == 1:
            logger.info('Attempting to delete infra with ID %s', infra['id'])
            if destroy.delete(infra['id']):
                logger.info('Successfully deleted infrastructure with ID %s', infra['id'])

def remove_old_entries(db, state):
    """
    Remove old entries from the DB
    """
    infras = db.deployment_get_infra_in_state_cloud(state)
    logger.info('Found %d infrastructures in state %s', len(infras), state)
    for infra in infras:
        if time.time() - infra['updated'] > int(CONFIG.get('cleanup', 'remove_after')):
            logger.info('Removing infrastructure %s from DB', infra['id'])
            db.deployment_log_remove(infra['id'])
            db.deployment_remove(infra['id'])

def delete_stuck_infras(db, state):
    """
    Delete any infras stuck in the accepted or creating state
    """
    infras = db.deployment_get_infra_in_state_cloud(state)
    logger.info('Found %d infrastructures in state %s', len(infras), state)
    for infra in infras:
        if time.time() - infra['updated'] > int(CONFIG.get('cleanup', 'delete_stuck_infras_after')):
            logger.info('Setting status of infrastructure %s to deleted', infra['id'])
            db.deployment_update_status(infra['id'], 'deletion-requested')
            db.deployment_update_status_reason(infra['id'], 'DeploymentFailed')

if __name__ == "__main__":
    while True:
        logger.info('Connecting to the DB')
        db = database.get_db()
        if db.connect():
            logger.info('Removing any old entries from the DB')
            remove_old_entries(db, 'deleted')
            remove_old_entries(db, 'unable')

            logger.info('Checking for infrastructure stuck in the creating state')
            delete_stuck_infras(db, 'creating')

            logger.info('Removing old failures from database')
            db.del_old_deployment_failures(24*60*60)

            logger.info('Retrying any incomplete deletions')
            retry_incomplete_deletions(db, 'deletion-failed')
            retry_incomplete_deletions(db, 'deleting')
            retry_incomplete_deletions(db, 'deletion-requested')

            db.close()
        else:
            logger.critical('Unable to connect to database')

        time.sleep(int(CONFIG.get('polling', 'cleaning')))
