#!/usr/bin/env python
"""Periodic cleaning of infrastructure and the database"""

from __future__ import print_function
import configparser
import logging
from logging.handlers import RotatingFileHandler
import re
import os
import sys
import time

from imc import config
from imc import cloud_utils
from imc import im_utils
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

def find_invalid_im_infras(db):
    """
    """
    # Get list of infras in configured state
    infras = db.deployment_get_infra_in_state_cloud('configured')

    # Get list of identities
    identities = []
    for infra in infras:
        if infra['identity'] not in identities:
            identities.append(infra['identity'])

    for identity in identities:
        clouds_info_list = cloud_utils.create_clouds_list(db, identity)

        for infra in infras:
            if infra['identity'] != identity:
                continue

            # Get the cloud name
            (im_infra_id, _, cloud, _, _) = db.deployment_get_im_infra_id(infra['id'])

            # Get a token if necessary
            token = tokens.get_token(cloud, identity, db, clouds_info_list)

            # Create the IM auth & client
            im_auth = im_utils.create_im_auth(cloud, token, clouds_info_list)
            client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
            (status, msg) = client.getauth()
            if status != 0:
                logger.critical('Error reading IM auth file: %s', msg)
                return False

            # Ge the state
            (state, details) = client.getstate(im_infra_id, 30)
            print('Infrastructure', infra['id'], 'on cloud', cloud, 'is in state', state)
            if state == 'stopped':
                logger.info('Infra with id %s on cloud %s is in the stopped state, so deleting...', infra['id'], cloud)
                db.deployment_update_status(unique_id, 'deletion-requested')

def find_unexpected_im_infras(db):
    """
    Find IM infrastructures which should not exist, typically deletion failed in some way 
    and new infrastructure was deployed in its place. This situation should be rare.
    """
    # Setup access to IM - we don't need to supply any cloud credentials initially because
    # we don't want to get updated statuses
    im_auth = im_utils.create_im_auth(None, None, None)
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return False

    # Get list of IM infrastructure IDs
    (status, ids) = client.list_infra_ids(10)

    if not ids:
        return True

    im_infras_to_delete = {}
    for uri in ids:
        pieces = uri.split('/')
        im_id = pieces[len(pieces) - 1]

        # Check if we know about the infrastructure
        (infra_id, status, cloud) = db.get_infra_from_im_infra_id(im_id)

        if infra_id:
            continue

        logger.info('Found unknown infrastructure with IM ID %s', im_id)

        # Check the deployment log, otherwise search the RADL for PROMINENCE_INFRASTRUCTURE_ID
        infra_id = db.check_im_deployment(im_id)
        cloud_from_data = None

        if infra_id:
            logger.info('From log found that IM id %s is associated with infra id %s', im_id, infra_id)
        else:
            (data, _) = client.getdata(im_id, 10)
            if not data:
                continue

            match = re.search("PROMINENCE_INFRASTRUCTURE_ID=([0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})", data['data'])
            if not match:
                logger.info('- could not get my associated infrastructure id from PROMINENCE_INFRASTRUCTURE_ID in data')
                continue

            infra_id = match.group(1)
            logger.info('From RADL found that IM id %s is associated with infra id %s', im_id, infra_id)

            match = re.search(r'\\\\\\"id\\\\\\": \\\\\\"([\w\-]+)\\\\\\"', data['data'])
            if match:
                cloud_from_data = match.group(1)
                logger.info('found cloud %s from data', cloud_from_data)

        (_, my_infra_status, cloud, _, _) = db.deployment_get_im_infra_id(infra_id)
        identity = db.deployment_get_identity(infra_id)
        logger.info('- this IM infrastructure is associated with my infrastructure id %s which has status %s identity %s', infra_id, my_infra_status, identity)

        if (not cloud or cloud == 'none') and cloud_from_data:
            cloud = cloud_from_data
        if not identity or identity == 'none':
            identity = '0d31ecc87b9c1b0c6f407a8884f41bfbb6da4092fa08b64beb313634a6ea7bf1@egi.eu'
           
        if not cloud or cloud == 'none' or identity == 'none':
            logger.info(' - this IM infrastructure %s has no known cloud or identity associated with it', infra_id)
        else:
            if my_infra_status in ('deleted', 'deleting', 'deletion-failed', 'deletion-required', 'unable'):
                im_infras_to_delete[my_infra_id] = {}
                im_infras_to_delete[my_infra_id]['identity'] = identity
                im_infras_to_delete[my_infra_id]['im_id'] = im_id
                im_infras_to_delete[my_infra_id]['cloud'] = cloud

    # Delete any infras
    if len(im_infras_to_delete) > 0:
        logger.info('Have %d infrastructures to delete', len(im_infras_to_delete))

    for infra in im_infras_to_delete:
        logger.info('Working on infra with my id=%s', infra)
        if delete_from_im(im_infras_to_delete[infra]['im_id'], db, im_infras_to_delete[infra]['identity'], im_infras_to_delete[infra]['cloud']):
            logger.info('- successfully deleted infrastructure with IM id %s for identity %s on cloud %s', im_infras_to_delete[infra]['im_id'], im_infras_to_delete[infra]['identity'], im_infras_to_delete[infra]['cloud'])
        else:
            logger.info('- unable to delete infrastructure with IM id %s for identity %s on cloud %s', im_infras_to_delete[infra]['im_id'], im_infras_to_delete[infra]['identity'], im_infras_to_delete[infra]['cloud'])


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
            db.deployment_update_status(unique_id, 'deletion-requested')
            db.deployment_update_status_reason(unique_id, 'DeploymentFailed')

def delete_from_im(im_infrastructure_id, db, identity, cloud):
    """
    Delete infrastructure from IM
    """
    logger.info('Inside delete_from_im with identity=%s, cloud=%s', identity, cloud)
    clouds_info_list = cloud_utils.create_clouds_list(db, identity)
    token = tokens.get_token(cloud, identity, db, clouds_info_list)
    im_auth = im_utils.create_im_auth(cloud, token, clouds_info_list)
    print('im_auth=', im_auth)
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return False

    (status, msg) = client.destroy(im_infrastructure_id, 60)
    if status == 0:
        return True
    else:
        logger.error('Failed to destroy infrastructure with id %s due to: %s', im_infrastructure_id, msg)
    return False

if __name__ == "__main__":
    while True:
        logger.info('Connecting to the DB')
        db = database.get_db()
        if db.connect():

            #logger.info('Checking for invalid IM infras')
            #find_invalid_im_infras(db)
            #db.close()
            #exit(0)

            logger.info('Removing any old entries from the DB')
            remove_old_entries(db, 'deleted')
            remove_old_entries(db, 'unable')

            logger.info('Checking for infrastructure stuck in the creating state')
            delete_stuck_infras(db, 'creating')

            logger.info('Removing old failures from database')
            db.del_old_deployment_failures(24*60*60)

            logger.info('Checking for unexpected IM infrastructures')
            find_unexpected_im_infras(db)

            logger.info('Retrying any incomplete deletions')
            retry_incomplete_deletions(db, 'deletion-failed')
            retry_incomplete_deletions(db, 'deleting')
            retry_incomplete_deletions(db, 'deletion-requested')

            db.close()

        exit(0)
        #time.sleep(int(CONFIG.get('polling', 'cleaning')))
