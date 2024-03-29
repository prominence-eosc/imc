"""Check if clouds are functional"""
from __future__ import print_function
import logging

from imc import appdbclient
from imc import config
from imc import tokens
from imc import utilities
from imc import cloud_utils

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def update_appdb_status(db, identity):
    """
    Update status from AppDB
    """
    sites_status = {}
    if CONFIG.get('features', 'enable_appdb'):
        logger.info('Getting cloud status from AppDB')
        sites_status = appdbclient.get_cloud_status_appdb()

    new_cloud_info = cloud_utils.create_clouds_list_egi(db, identity)
    if not new_cloud_info:
        logger.info('No resources info for DB')
        return

    for new_cloud in new_cloud_info:
        name = new_cloud['name']
        logger.info('Checking cloud %s', name)

        # Initialize entry in info table if necessary
        db.init_cloud_info(name, identity)

        # Update cloud status if necessary
        for site in sites_status:
            if site in name:
                if sites_status[site] == 'OK':
                    logger.info('Setting cloud %s monitoring status in DB to up', name)
                    db.set_cloud_mon_status(name, identity, 0)
                else:
                    logger.info('Setting cloud %s monitoring status in DB to down', name)
                    db.set_cloud_mon_status(name, identity, 1)

def update_clouds_status(db, identity, config):
    """
    Update status of each cloud
    """
    for cloud_info in config:
        name = cloud_info['name']

        if cloud_info['type'] != 'cloud':
            continue

        logger.info('Checking cloud %s', name)

        # Check if a token is necessary
        token_required = False
        if 'credentials' in cloud_info:
            if 'token' in cloud_info['credentials']:
                if cloud_info['credentials']['token']:
                    token_required = True

        # Get a token if necessary
        token = tokens.get_token(name, identity, db, config)

        if not token and token_required:
            logger.error('Unable to get token so cannot check if cloud %s is functional', name)
            continue

        retryme = False
        try:
            status = check_cloud(name, cloud_info, token)
        except:
            logger.info('Setting status of cloud %s to down due to timeout', name)
            db.set_cloud_status(name, identity, 1)
        else:
            if not status:
                logger.info('Setting status of cloud %s to down, will retry', name)
                retryme = True
                db.set_cloud_status(name, identity, 1)
            else:
                logger.info('Cloud %s is functional', name)
                db.set_cloud_status(name, identity, 0)
          
        if retryme:
            try:
                status = check_cloud(name, cloud_info, token)
            except:
                logger.info('Setting status of cloud %s to down due to timeout', name)
                db.set_cloud_status(name, identity, 1)
            else:
                if not status:
                    logger.info('Setting status of cloud %s to down SECOND ATTEMPT', name)
                    db.set_cloud_status(name, identity, 1)
                else:
                    db.set_cloud_status(name, identity, 0)

def check_cloud(cloud, config, token):
    """
    Check if a cloud is functional by listing locations
    """
    # Connect to the cloud
    conn = cloud_utils.connect_to_cloud(cloud, config, token)
    if not conn:
        return False

    # List locations (it's quick!)
    images = None
    try:
        images = conn.list_images()
    except Exception as ex:
        logger.warning('Unable to list locations on cloud %s due to %s', cloud, ex)
        return False

    if not images:
        logger.warning('Unable to list images on cloud %s', cloud)

    return True

