"""Deploy infrastructure on the specified cloud, with extensive error handling"""

from __future__ import print_function
import os
import sys
from string import Template
import time
import random
import logging
import configparser

from imc import config
from imc import database
from imc import destroy
from imc import imclient
from imc import tokens
from imc import utilities
from imc import im_utils
from imc import cloud_utils

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def update_im_client(client, cloud, identity, db, clouds_info_list):
    """
    Setup IM client ready to access the specified cloud
    """
    # Check & get auth token if necessary
    token = tokens.get_token(cloud, identity, db, clouds_info_list)

    # Create auth for IM and update client
    im_auth = im_utils.create_im_auth(cloud, token, clouds_info_list)
    (status, msg) = client.getauth(im_auth)
    if status != 0:
        logger.critical('Error reading IM auth file in update_im_client: %s', msg)

def deploy(radl, cloud, time_begin, unique_id, identity, db, num_nodes=1, used_cpus=1, used_memory=1):
    """
    Deploy infrastructure from a specified RADL file
    """
    # Get full list of cloud info
    clouds_info_list = cloud_utils.create_clouds_list(db, identity)

    # Setup Infrastructure Manager client
    client = imclient.IMClient(url=CONFIG.get('im', 'url'))

    # Set availability zone in RADL if necessary TODO: remove OPA
    #cloud_info = opa_client.get_cloud(cloud)
    #if 'availability_zones' in cloud_info:
    #    availability_zones = cloud_info['availability_zones']
    #    if availability_zones:
    #        random.shuffle(availability_zones)
    #        logger.info('Using availability zone %s', availability_zones[0])
    #        radl_base = utilities.set_availability_zone(radl_base, availability_zones[0])

    retries_per_cloud = int(CONFIG.get('deployment', 'retries'))
    retry = 0
    success = False
    time_begin_this_cloud = time.time()

    # Retry loop
    while retry < retries_per_cloud + 1 and not success:
        if retry > 0:
            time.sleep(int(CONFIG.get('polling', 'duration')))
        logger.info('Deployment attempt %d of %d', retry+1, retries_per_cloud+1)
        retry += 1

        # Check if we should stop
        (im_infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_im_infra_id(unique_id)
        if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
            logger.info('Deletion requested of infrastructure, aborting deployment')
            return (None, None)

        # Create infrastructure
        update_im_client(client, cloud, identity, db, clouds_info_list)
        (infrastructure_id, msg) = client.create(radl, int(CONFIG.get('timeouts', 'creation')))

        if infrastructure_id:
            logger.info('Created infrastructure on cloud %s with IM id %s and waiting for it to be configured', cloud, infrastructure_id)
            if not db.create_im_deployment(unique_id, infrastructure_id):
                logger.critical('Unable to add IM infrastructure ID %s for infra with id %s to deployments log', infrastructure_id, unique_id)

            # Set the cloud & IM infrastructure ID
            db.deployment_update_status(unique_id, None, cloud, infrastructure_id)

            # Set the resources used by this infrastructure
            db.deployment_update_resources(unique_id, num_nodes, used_cpus, used_memory)

            # Change the status
            db.deployment_update_status(unique_id, 'creating')

            time_created = time.time()
            count_unconfigured = 0
            state_previous = None

            # Wait for infrastructure to enter the configured state
            while True:
                # Sleep
                time.sleep(int(CONFIG.get('polling', 'duration')))

                # Check if we should stop
                (im_infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
                    logger.info('Deletion requested of infrastructure so aborting deployment')
                    return (None, None)

                # Don't spend too long trying to create infrastructure, give up eventually
                if time.time() - time_begin > int(CONFIG.get('timeouts', 'total')):
                    logger.info('Giving up, total time waiting is too long, so will destroy infrastructure with IM id %s', infrastructure_id)
                    db.set_deployment_failure(cloud, identity, 5, time.time()-time_begin)
                    update_im_client(client, cloud, identity, db, clouds_info_list)
                    destroy.destroy(client, infrastructure_id)
                    return (None, None)

                # Get the current overall state & states of all VMs in the infrastructure
                update_im_client(client, cloud, identity, db, clouds_info_list)
                (states, msg) = client.getstates(infrastructure_id, int(CONFIG.get('timeouts', 'status')))
                logger.info('InfraID=%s IM_ID=%s has state: %s', unique_id, infrastructure_id, msg)

                # If state is not known, wait
                if not states:
                    logger.info('State is not known for infrastructure with id %s on cloud %s', infrastructure_id, cloud)
                    continue

                # Overall state of infrastructure
                state = None
                have_nodes = -1
                if 'state' in states:
                    if 'state' in states['state']:
                        state = states['state']['state']
                    if 'vm_states' in states['state']:
                        have_nodes = len(states['state']['vm_states'])
                
                # If the state or number of nodes is unknown, wait
                if not state or have_nodes == -1:
                    logger.warning('Unable to determine state and/or number of VMs from IM')
                    continue

                # Log a change in state
                if state != state_previous:
                    logger.info('Infrastructure with IM id %s is in state %s', infrastructure_id, state)
                    state_previous = state

                # Handle difference situation when state is configured
                if state == 'configured':
                    # The final configured state
                    logger.info('Successfully configured infrastructure on cloud %s, took %d secs', cloud, time.time() - time_begin_this_cloud)
                    db.set_deployment_failure(cloud, identity, 0, time.time()-time_begin_this_cloud)
                    success = True
                    return (infrastructure_id, None)

                # Destroy infrastructure which is taking too long to enter the configured state
                if time.time() - time_created > int(CONFIG.get('timeouts', 'configured')):
                    logger.warning('Waiting too long for infrastructure to be configured, so destroying')
                    db.set_deployment_failure(cloud, identity, 3, time.time()-time_created)
                    update_im_client(client, cloud, identity, db, clouds_info_list)
                    destroy.destroy(client, infrastructure_id)
                    break

                # Destroy infrastructure which is taking too long to enter the running state
                if time.time() - time_created > int(CONFIG.get('timeouts', 'notrunning')) and state != 'running' and state != 'unconfigured':
                    logger.warning('Waiting too long for infrastructure to enter the running state, so destroying')
                    db.set_deployment_failure(cloud, identity, 2, time.time()-time_created)
                    update_im_client(client, cloud, identity, db, clouds_info_list)
                    destroy.destroy(client, infrastructure_id)
                    break

                # Destroy infrastructure for which deployment failed
                if state == 'failed':
                    logger.warning('Infrastructure creation failed on cloud %s, so destroying', cloud)

                    # Get the full data about the infrastructure from IM, as we can use it to determine what
                    # caused some failures
                    (_, msg) = client.getdata(infrastructure_id, int(CONFIG.get('timeouts', 'status')))
                    fatal_failure = False
                    reason = None

                    if '403 Forbidden Quota' in msg:
                        logger.info('Infrastructure creation failed due to quota exceeded on cloud %s, our id=%s, IM id=%s', cloud, unique_id, infrastructure_id)
                        db.set_deployment_failure(cloud, identity, 6, time.time()-time_created)
                        fatal_failure = True
                        reason = 'QuotaExceeded'
                    elif 'No image found with ID' in msg:
                        logger.info('Infrastructure creation failed due to image not found on cloud %s, our id=%s, IM id=%s', cloud, unique_id, infrastructure_id)
                        db.set_deployment_failure(cloud, identity, 7, time.time()-time_created)
                        fatal_failure = True
                        reason = 'ImageNotFound'
                    else:
                        db.set_deployment_failure(cloud, identity, 1, time.time()-time_created)

                    update_im_client(client, cloud, identity, db, clouds_info_list)
                    destroy.destroy(client, infrastructure_id)

                    # In the event of a fatal failure there's no reason to try again
                    if fatal_failure:
                        return (None, reason)

                    break

                # Handle unconfigured infrastructure
                if state == 'unconfigured':
                    count_unconfigured += 1
                    file_unconf = '%s/contmsg-%s-%d.txt' % (CONFIG.get('logs', 'contmsg'), unique_id, time.time())
                    contmsg = client.getcontmsg(infrastructure_id, int(CONFIG.get('timeouts', 'deletion')))
                    if count_unconfigured < int(CONFIG.get('deployment', 'reconfigures')) + 1:
                        logger.warning('Infrastructure on cloud %s is unconfigured, will try reconfiguring after writing contmsg to a file', cloud)
                        try:
                            with open(file_unconf, 'w') as unconf:
                                unconf.write(contmsg)
                        except Exception as error:
                            logger.warning('Unable to write contmsg to file')
                        update_im_client(client, cloud, identity, db, clouds_info_list)
                        client.reconfigure(infrastructure_id, int(CONFIG.get('timeouts', 'reconfigure')))
                    else:
                        logger.warning('Infrastructure has been unconfigured too many times, so destroying after writing contmsg to a file')
                        db.set_deployment_failure(cloud, identity, 4, time.time()-time_created)
                        try:
                            with open(file_unconf, 'w') as unconf:
                                unconf.write(contmsg)
                        except Exception as error:
                            logger.warning('Unable to write contmsg to file')
                        update_im_client(client, cloud, identity, db, clouds_info_list)
                        destroy.destroy(client, infrastructure_id)
                        break
        else:
            logger.warning('Deployment failure on cloud %s with id %s with msg="%s"', cloud, infrastructure_id, msg)
            if msg == 'timedout':
                logger.warning('Infrastructure creation failed due to a timeout')
                db.set_deployment_failure(cloud, identity, 4, time.time()-time_created)
            else:
                file_failed = '%s/failed-%s-%d.txt' % (CONFIG.get('logs', 'contmsg'), unique_id, time.time())
                db.set_deployment_failure(cloud, identity, 4, time.time()-time_created)
                logger.warning('Infrastructure creation failed, writing stdout/err to file "%s"', file_failed)
                try:
                    with open(file_failed, 'w') as failed:
                        failed.write(msg)
                except Exception as error:
                    logger.warning('Unable to write contmsg to file')

    return (None, None)

