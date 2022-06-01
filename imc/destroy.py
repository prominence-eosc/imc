"""Destroy the specified infrastructure, with retries"""
from __future__ import print_function
import re
import time
import logging
import configparser

from imc import config
from imc import cloud_utils
from imc import database
from imc import tokens
from imc import utilities
from imc import resources

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def destroy(client, infrastructure_id):
    """
    Destroy the specified infrastructure, including retries
    """
    count = 0
    delay_factor = float(CONFIG.get('deletion', 'factor'))
    delay = delay_factor
    destroyed = False
    while not destroyed and count < int(CONFIG.get('deletion', 'retries')):
        status = client.delete_instance(infrastructure_id)
        if status:
            destroyed = True
            break

        count += 1
        delay = delay*delay_factor
        time.sleep(int(count + delay))

    if destroyed:
        logger.info('Destroyed infrastructure with id %s', infrastructure_id)
    else:
        logger.critical('Unable to destroy infrastructure with id %s', infrastructure_id)

    return destroyed

def delete(unique_id):
    """
    Delete the infrastructure with the specified id
    """
    logger.info('Deleting infrastructure with id %s', unique_id)

    db = database.get_db()
    db.connect()

    (infra_id, infra_status, cloud, _, _) = db.deployment_get_infra_id(unique_id)
    logger.info('Obtained cloud infrastructure id %s and cloud %s and status %s', infra_id, cloud, infra_status)

    if infra_id and cloud:
        logger.info('Deleting cloud infrastructure with infrastructure id %s', infra_id)

        # Get the identity of the user who created the infrastructure
        identity = db.deployment_get_identity(unique_id)

        # Get cloud details
        clouds_info_list = cloud_utils.create_clouds_list(db, identity)
        for cloud_info in clouds_info_list:
            if cloud_info['name'] == cloud:
                info = cloud_info
                break
     
        # Check & get auth token if necessary
        token = tokens.get_token(cloud, identity, db, clouds_info_list)
        info = tokens.get_openstack_token(token, info)

        # Setup Resource client
        client = resources.Resource(info)

        # Delete the infrastructure, with retries
        destroyed = destroy(client, infra_id)

        if destroyed:
            db.deployment_update_status(unique_id, 'deleted')
            logger.info('Destroyed infrastructure with infrastructure id %s', infra_id)
        else:
            db.deployment_update_status(unique_id, 'deletion-failed')
            logger.critical('Unable to destroy infrastructure with infrastructure id %s', infra_id)
            return False
    else:
        logger.info('No need to destroy infrastructure because resource infrastructure id is %s, resource name is %s', infra_id, cloud)
        db.deployment_update_status(unique_id, 'deleted')

    db.close()
    return True

