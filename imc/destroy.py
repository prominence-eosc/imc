"""Destroy the specified IM infrastructure, with retries"""

from __future__ import print_function
import re
import time
import logging
import configparser

from imc import config
from imc import database
from imc import destroy
from imc import batchclient
from imc import imclient
from imc import opaclient
from imc import tokens
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def destroy(client, infrastructure_id):
    """
    Destroy the specified infrastructure, including retries since clouds can be unreliable
    """
    count = 0
    delay_factor = float(CONFIG.get('deletion', 'factor'))
    delay = delay_factor
    destroyed = False
    while not destroyed and count < int(CONFIG.get('deletion', 'retries')):
        (return_code, msg) = client.destroy(infrastructure_id, int(CONFIG.get('timeouts', 'deletion')))
        if return_code == 0:
            destroyed = True
        count += 1
        delay = delay*delay_factor
        time.sleep(int(count + delay))

    if destroyed:
        logger.info('Destroyed infrastructure with IM id %s', infrastructure_id)
    else:
        logger.critical('Unable to destroy infrastructure with IM id %s due to: "%s"', infrastructure_id, msg)

    return destroyed

def delete(unique_id, batch_client):
    """
    Delete the infrastructure with the specified id
    """
    logger.info('Deleting infrastructure with id %s', unique_id)

    db = database.get_db()
    db.connect()

    (im_infra_id, infra_status, cloud, _, _) = db.deployment_get_im_infra_id(unique_id)
    logger.info('Obtained IM id %s and cloud %s and status %s', im_infra_id, cloud, infra_status)

    # Get full list of cloud info
    clouds_info_list = utilities.create_clouds_list(CONFIG.get('clouds', 'path'))

    # Deterime resource type
    resource_type = None
    for cloud_info in clouds_info_list:
        if cloud_info['name'] == cloud:
            resource_type = cloud_info['type']

    if im_infra_id and cloud:
        if resource_type == 'cloud':
            match_obj_name = re.match(r'\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b', im_infra_id)
            if match_obj_name:
                logger.info('Deleting cloud infrastructure with IM id %s', im_infra_id)

                # Get the identity of the user who created the infrastructure
                identity = db.deployment_get_identity(unique_id)
     
                # Check & get auth token if necessary
                token = tokens.get_token(cloud, identity, db, clouds_info_list)

                # Setup Infrastructure Manager client
                im_auth = utilities.create_im_auth(cloud, token, clouds_info_list)
                client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
                (status, msg) = client.getauth()
                if status != 0:
                    logger.critical('Error reading IM auth file: %s', msg)
                    db.close()
                    return False

                destroyed = destroy(client, im_infra_id)

                if destroyed:
                    db.deployment_update_status_with_retries(unique_id, 'deleted')
                    logger.info('Destroyed infrastructure with IM infrastructure id %s', im_infra_id)
                else:
                    db.deployment_update_status_with_retries(unique_id, 'deletion-failed')
                    logger.critical('Unable to destroy infrastructure with IM infrastructure id %s', im_infra_id)
                    return False
            else:
                logger.critical('IM infrastructure id %s does not match regex', im_infra_id)
                db.deployment_update_status_with_retries(unique_id, 'deleted')
        elif resource_type == 'batch':
            logger.info('Deleting batch infrastructure with job id %s', im_infra_id)
            batch_client.destroy(im_infra_id, cloud)
            time.sleep(2)
            logger.info('Batch job with id %s now in status %s', im_infra_id, batch_client.getstate(im_infra_id, cloud))
    else:
        logger.info('No need to destroy infrastructure because resource infrastructure id is %s, resource name is %s, resource type is %s', im_infra_id, cloud, resource_type)
        db.deployment_update_status_with_retries(unique_id, 'deleted')
    db.close()
    return True

