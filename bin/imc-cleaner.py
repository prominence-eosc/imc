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
from imc import tokens
from imc import utilities
from imc import resources

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
            if destroy.delete(db, infra['id']):
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

def find_and_delete_phantoms(db):
    """
    Find and delete any phantom infrastructures on each resource
    """
    # Get full list of cloud info
    clouds_info_list = cloud_utils.create_clouds_list(db, 'static', True)

    for cloud in clouds_info_list:
        # Get a token if necessary
        logger.info('Getting a new token if necessary')
        token = tokens.get_token(cloud['name'], 'static', db, clouds_info_list)
        cloud = tokens.get_openstack_token(token, cloud)

        # Get list of VMs on the resource
        client = resources.Resource(config)
        instances = client.list_instances()
        
        for instance in instances:
            # Check if we know about the instance
            (infra_id, status, cloud) = db.get_infra_from_im_infra_id(instance['id'])

            # Ignore valid instances
            if infra_id and status in ('configured', 'creating'):
                logger.info('Found valid instance with id %s associated with cloud %s with status %s', infra_id, cloud, status)
                continue

            if not infra_id:
                logger.info('Found unknown instance %s, %s on cloud %s', instance['name'], instance['id'], cloud['name'])

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

            logger.info('Finding any phantom infrastructures')
            find_and_delete_phantoms(db)

            db.close()
        else:
            logger.critical('Unable to connect to database')

        time.sleep(int(CONFIG.get('polling', 'cleaning')))
