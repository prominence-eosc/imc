""" Consistency checks """

import ConfigParser
import logging
from logging.handlers import RotatingFileHandler
import re
import os
import sys
import time

import database
import imc
import imclient
import tokens
import utilities

# Configuration
CONFIG = ConfigParser.ConfigParser()
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
logger = logging.getLogger('checks')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def get_db():
    """
    Prepare DB
    """
    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))
    return db

def find_unexpected_im_infras():
    """
    Find IM infrastructures which should not exist
    """
    # Get full list of cloud info
    clouds_info_list = utilities.create_clouds_list(CONFIG.get('clouds', 'path'))

    # Get list of IM infrastructure IDs
    im_auth = utilities.create_im_auth(None, None, clouds_info_list)
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return False
    (status, ids) = client.list_infra_ids(10)

    if ids:
        db = get_db()
        if db.connect():
            for uri in ids:
                pieces = uri.split('/')
                im_id = pieces[len(pieces) - 1]

                # Check if we know about the infrastructure
                (infra_id, status, cloud) = db.get_infra_from_im_infra_id(im_id)
                if not infra_id:
                    logger.info('Found unknown infrastructure with IM ID %s', im_id)
                    (_, data) = client.getdata(im_id, 10)
                    if data:
                        for cloud_info in clouds_info_list:
                            cloud_name = cloud_info['name']
                            if cloud_name in data:
                                logger.info('Found unknown IM id %s on cloud %s, deleting...', im_id, cloud_name)
                                if delete_from_im(im_id, cloud_name):
                                    logger.info('Successfully deleted infrastructure with IM id %s on cloud %s', im_id, cloud_name)
                                break
                else:
                    logger.info('Found IM id %s on cloud %s with status %s and our id %s', im_id, cloud, status, infra_id)
            db.close()

def retry_failed_deletions():
    """
    Retry failed deletions
    """
    db = get_db()
    if db.connect():
        infras = db.deployment_get_infra_in_state_cloud('deletion-failed', None) 
        for infra in infras:
            if time.time() - infra['updated'] > 12*60*60:
                logger.info('Attempting to delete infra with ID %s', infra['id'])
                if imc.delete(infra['id']) == 0:
                    logger.info('Successfully deleted infrastructure with ID %s', infra['id'])
        db.close()

def delete_from_im(im_infrastructure_id, cloud):
    """
    Delete infrastructure from IM
    """
    db = get_db()
    if db.connect():
        clouds_info_list = utilities.create_clouds_list(CONFIG.get('clouds', 'path'))
        token = tokens.get_token(cloud, db, clouds_info_list)
        db.close()
        im_auth = utilities.create_im_auth(cloud, token, clouds_info_list)
        client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
        (status, msg) = client.getauth()
        if status != 0:
            logger.critical('Error reading IM auth file: %s', msg)
            return False
        (status, _) = client.destroy(im_infrastructure_id, 30)
        if status == 0:
            return True
        return False

if __name__ == "__main__":
    # Check for unexpected IM infrastructures
    logger.info('Checking for unexpected IM infrastructures')
    find_unexpected_im_infras()

    # Retry failed deletions
    logger.info('Retrying any failed deletions')
    retry_failed_deletions()

