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
from imc import workers
from imc import resources

# Configuration
CONFIG = config.get_config()

# Logging
handler = RotatingFileHandler(filename=CONFIG.get('logs', 'filename').replace('imc.log', 'cleaner.log'),
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

def find_and_delete_phantoms(db, workers):
    """
    Find and delete any phantom infrastructures on each resource
    """
    # Get list of all identities which have run jobs recently
    identities = db.deployment_get_identities()

    for identity in identities:
        # Get full list of cloud info
        clouds_info_list = cloud_utils.create_clouds_list(db, identity, True)

        for cloud in clouds_info_list:
            # Get a token if necessary
            logger.info('Getting a new token if necessary for cloud %s', cloud['name'])
            token = tokens.get_token(cloud['name'], identity, db, clouds_info_list)
            cloud = tokens.get_openstack_token(token, cloud)

            # Get list of VMs on the resource
            client = resources.Resource(cloud)
            instances = client.list_instances()
            logger.info('Found %d instances on cloud %s', len(instances), cloud['name'])
        
            for instance in instances:
                infra_id_from_cloud = None
                unique_infra_id_from_cloud = None
                if 'prominence-infra-id' in instance['metadata']:
                    infra_id_from_cloud = instance['metadata']['prominence-infra-id']
                if 'prominence-unique-infra-id' in instance['metadata']:
                    unique_infra_id_from_cloud = instance['metadata']['prominence-unique-infra-id']

                if infra_id_from_cloud and unique_infra_id_from_cloud:
                    (_, status, _, _, _) = db.deployment_get_infra_id(infra_id_from_cloud)

                    if status not in ('creating', 'running', 'visible', 'left', 'deletion-requested', 'deleting'):
                        logger.info('Found unexpected infrastructure on cloud %s with status %s', cloud['name'], status)

                        # Check if the instance is in the pool
                        found = False
                        for worker in workers:
                            if worker['ProminenceUniqueInfrastructureId'] == unique_infra_id_from_cloud:
                                found = True

                        # If not in the pool, delete the instance
                        if not found:
                            logger.info('Unexpected instance with name %s and id %s will be deleted',
                                        instance['name'],
                                        instance['id'])
                            db.deployment_update_status(infra_id_from_cloud, 'deletion-requested')

if __name__ == "__main__":
    while True:
        logger.info('Connecting to the DB')
        db = database.get_db()
        if db.connect():
            logger.info('Removing any old entries from the DB')
            remove_old_entries(db, 'deleted-validated')
            remove_old_entries(db, 'unable')

            logger.info('Checking for infrastructure stuck in the creating state')
            delete_stuck_infras(db, 'creating')

            logger.info('Retrying any incomplete deletions')
            retry_incomplete_deletions(db, 'deletion-failed')
            retry_incomplete_deletions(db, 'deleting')
            retry_incomplete_deletions(db, 'deletion-requested')

            logger.info('Finding any phantom infrastructures')
            workers_list = workers.get_workers()
            find_and_delete_phantoms(db, workers_list)

            db.close()
        else:
            logger.critical('Unable to connect to database')

        time.sleep(int(CONFIG.get('polling', 'cleaning')))
