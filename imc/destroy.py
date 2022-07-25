"""Destroy the specified infrastructure, with retries"""
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

def delete(db, infra_id):
    """
    Delete the infrastructure(s) with the specified id
    """
    # Get the identity of the user who created the infrastructure
    identity = db.deployment_get_identity(infra_id)

    # Get cloud details
    clouds_info_list = cloud_utils.create_clouds_list(db, identity)

    logger.info('Deleting infrastructure(s) with id %s for identity %s', infra_id, identity)

    overall_status = True

    infrastructures = db.get_deployments(infra_id)
    for infrastructure in infrastructures:
        if not infrastructure['id'] or not infrastructure['cloud']:
            logger.info('No need to destroy infrastructure with unique id %s because it was never successfully created', infrastructure['unique_id'])
        else:
            logger.info('Obtained cloud infrastructure id %s and cloud %s and unique id %s', infrastructure['id'], infrastructure['cloud'], infrastructure['unique_id'])
            
            name = 'prominence-%s' % infrastructure['unique_id']

            # Get details of the required cloud
            info = None
            for cloud_info in clouds_info_list:
                if cloud_info['name'] == infrastructure['cloud']:
                    info = cloud_info
                    break
     
            # Check & get auth token if necessary
            token = tokens.get_token(infrastructure['cloud'], identity, db, clouds_info_list)
            info = tokens.get_openstack_token(token, info)

            # Setup Resource client
            client = resources.Resource(info)

            # Delete the infrastructure
            status = client.delete_instance(name, infrastructure['id'])
            if not status:
                overall_status = False
                logger.critical('Unable to destroy infrastructure with infrastructure id %s', infrastructure['id'])
                db.deployment_update_status_log(infrastructure['unique_id'], 'deletion-failed')
            else:
                logger.info('Destroyed infrastructure with name %s and infrastructure id %s', name, infrastructure['id'])
                db.deployment_update_status_log(infrastructure['unique_id'], 'deleted')

    if overall_status:
        db.deployment_update_status(infra_id, 'deleted')
        logger.info('Destroyed infrastructure %s', infra_id)
    else:
        db.deployment_update_status(infra_id, 'deletion-failed')
        logger.critical('Deletion of infrastructure %s failed', infra_id)
        return False

    return True
