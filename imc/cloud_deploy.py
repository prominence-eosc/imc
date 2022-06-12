"""Deploy infrastructure on the specified cloud, with extensive error handling"""

import os
import sys
from string import Template
import time
import random
import logging
import configparser
import uuid

from imc import config
from imc import database
from imc import tokens
from imc import utilities
from imc import cloud_utils
from imc import resources

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def deploy(image, flavor, disk, cloud, region, clouds_info_list, time_begin, unique_id, identity, db, worker_token, used_cpus=1, used_memory=1):
    """
    Deploy infrastructure on the specified cloud
    """
    # Get a token if necessary
    logger.info('Getting a new token if necessary')
    token = tokens.get_token(cloud, identity, db, clouds_info_list)
    for cloud_info in clouds_info_list:
        if cloud_info['name'] == cloud:
            break

    cloud_info = tokens.get_openstack_token(token, cloud_info)

    # Setup resource
    client = resources.Resource(cloud_info)

    # Initialisation
    retries_per_cloud = int(CONFIG.get('deployment', 'retries'))
    retry = 0
    success = False
    fatal_failure = False
    time_begin_this_cloud = time.time()

    # Retry loop
    while retry < retries_per_cloud + 1 and not success:
        if retry > 0:
            time.sleep(int(CONFIG.get('polling', 'duration')))
        logger.info('Deployment attempt %d of %d', retry+1, retries_per_cloud+1)
        retry += 1

        # Check if we should stop
        (infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_infra_id(unique_id)
        if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
            logger.info('Deletion requested of infrastructure, aborting deployment')
            return (None, None)

        # Network
        network = None
        if 'networks' in cloud_info:
            if cloud_info['networks']:
                network = random.choice(cloud_info['networks'])
                logging.info('Using network %s', network)

        # Security groups
        security_groups = None
        if 'security_groups' in cloud_info:
            if cloud_info['security_groups']:
                security_groups = cloud_info['security_groups']

        # Prepare for creating infrastructure
        unique_infra_id = str(uuid.uuid4())
        db.create_cloud_deployment(unique_id, unique_infra_id, cloud)
        name = 'prominence-%s' % unique_infra_id
        time_created = time.time()

        # Update userdata
        try:
            with open(CONFIG.get('infrastructure', 'userdata')) as fh:
                 userdata = fh.read()
        except Exception as err:
            logger.critical('Error reading userdata template due to: %s', err)
            return False

        try:
            userdata = Template(userdata).substitute(cloud=cloud,
                                                     region=region,
                                                     token=worker_token,
                                                     uid_infra=unique_id,
                                                     unique_uid_infra=unique_infra_id,
                                                     server_ip=CONFIG.get('server', 'ip'))
        except Exception as err:
            logger.critical('Error creating userdata from template due to: %s', err)
            return False

        # Create infrastructure
        (infrastructure_id, msg) = client.create_instance(name,
                                                          image,
                                                          flavor,
                                                          network,
                                                          security_groups,
                                                          userdata,
                                                          disk,
                                                          unique_id,
                                                          unique_infra_id)

        if infrastructure_id:
            logger.info('Created infrastructure on cloud %s with id %s and waiting for it to be deployed', cloud, infrastructure_id)
            db.update_cloud_deployment(unique_infra_id, infrastructure_id)

            # Set the cloud & infrastructure ID
            db.deployment_update_status(unique_id, None, cloud, infrastructure_id)

            # Set the resources used by this infrastructure
            db.deployment_update_resources(unique_id, 1, used_cpus, used_memory)

            # Change the status
            db.deployment_update_status(unique_id, 'creating')

            state_previous = None

            # Wait for infrastructure to enter the running state
            while True:
                # Sleep
                time.sleep(int(CONFIG.get('polling', 'duration')))

                # Check if we should stop
                (infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_infra_id(unique_id)
                if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
                    logger.info('Deletion requested of infrastructure so aborting deployment')
                    return (None, None)

                # Don't spend too long trying to create infrastructure, give up eventually
                if time.time() - time_begin > int(CONFIG.get('timeouts', 'total')):
                    logger.info('Giving up, total time waiting is too long, so will destroy infrastructure with infrastructure id %s', infrastructure_id)
                    db.set_deployment_stats(cloud, identity, 5, time.time()-time_begin)
                    client.delete_instance(name, infrastructure_id)
                    return (None, None)

                # Get the current state of the infrastructure
                state = client.get_instance(name, infrastructure_id)

                # If state is not known, wait
                if not state:
                    logger.info('State is not known for infrastructure with id %s on cloud %s', infrastructure_id, cloud)
                    continue

                # Log a change in state
                if state != state_previous:
                    logger.info('Infrastructure with id %s is in state %s', infrastructure_id, state)
                    state_previous = state

                # Handle difference situation when state is running
                if state == 'running':
                    logger.info('Successfully deployed infrastructure on cloud %s, took %d secs', cloud, time.time() - time_begin_this_cloud)
                    db.set_deployment_stats(cloud, identity, 0, time.time()-time_begin_this_cloud)
                    success = True
                    return (infrastructure_id, None)

                # Destroy infrastructure which is taking too long to enter the running state
                if time.time() - time_created > int(CONFIG.get('timeouts', 'notrunning')) and state != 'running':
                    logger.warning('Waiting too long for infrastructure to enter the running state, so destroying')
                    db.set_deployment_stats(cloud, identity, 2, time.time()-time_created)
                    client.delete_instance(name, infrastructure_id)
                    break

                # Destroy infrastructure for which deployment failed
                if state == 'failed' or state == 'error':
                    logger.warning('Infrastructure creation failed on cloud %s, so destroying', cloud)
                    db.set_deployment_stats(cloud, identity, 1, time.time()-time_created)
                    client.delete_instance(name, infrastructure_id)
                    break

        else:
            logger.warning('Deployment failure on cloud %s with id %s with msg="%s"', cloud, infrastructure_id, msg)

            if 'Quota exceeded' in msg:
                logger.info('Infrastructure creation failed due to quota exceeded on cloud %s, our id=%s', cloud, unique_id)
                db.set_deployment_stats(cloud, identity, 6, time.time()-time_created)
                fatal_failure = True
            elif 'Can not find requested image' in msg:
                logger.info('Infrastructure creation failed due to image not found on cloud %s, our id=%s', cloud, unique_id)
                db.set_deployment_stats(cloud, identity, 7, time.time()-time_created)
                fatal_failure = True
            elif 'Flavor' in msg and 'could not be found' in msg:
                logger.info('Infrastructure creation failed due to flavour not found on cloud %s, our id=%s', cloud, unique_id)
                db.set_deployment_stats(cloud, identity, 8, time.time()-time_created)
                fatal_failure = True
            elif 'InsufficientInstanceCapacity' in msg:
                logger.info('Infrastructure creation failed due to InsufficientInstanceCapacity on cloud %s, our id=%s', cloud, unique_id)
                db.set_deployment_stats(cloud, identity, 9, time.time()-time_created)
                fatal_failure = True
            elif 'Image' in msg and 'is not active' in msg:
                logger.info('Infrastructure creation failed due to image not active on cloud %s, our id=%s', cloud, unique_id)
                db.set_deployment_stats(cloud, identity, 7, time.time()-time_created)
                fatal_failure = True

            file_failed = '%s/failed-%s-%d.txt' % (CONFIG.get('logs', 'contmsg'), unique_id, time.time())
            logger.warning('Infrastructure creation failed, writing stdout/err to file "%s"', file_failed)

            try:
                with open(file_failed, 'w') as failed:
                    failed.write(msg)
            except Exception as err:
                logger.warning('Unable to write contmsg to file')

            # In the event of a fatal failure there's no reason to try again
            if fatal_failure:
                return (None, None)

    return (None, None)

