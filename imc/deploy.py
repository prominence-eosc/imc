"""Deploy infrastructure on the specified cloud, with extensive error handling"""

from __future__ import print_function
import os
import sys
from string import Template
import time
from random import shuffle
import logging
import configparser

from imc import destroy
from imc import imclient
from imc import opaclient
from imc import tokens
from imc import utilities

# Configuration
CONFIG = utilities.get_config()

# Logging
logger = logging.getLogger(__name__)

def deploy(radl, cloud, time_begin, unique_id, identity, db, num_nodes=1):
    """
    Deploy infrastructure from a specified RADL file
    """
    # Get full list of cloud info
    clouds_info_list = utilities.create_clouds_list(CONFIG.get('clouds', 'path'))

    # Check & get auth token if necessary
    token = tokens.get_token(cloud, identity, db, clouds_info_list)

    # Setup Open Policy Agent client
    opa_client = opaclient.OPAClient(url=CONFIG.get('opa', 'url'),
                                     timeout=int(CONFIG.get('opa', 'timeout')))

    # Setup Infrastructure Manager client
    im_auth = utilities.create_im_auth(cloud, token, clouds_info_list)
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return None

    # Create RADL content for initial deployment: for multiple nodes we strip out all configure/contextualize
    # blocks and will add this back in once we have successfully deployed all required VMs
    if num_nodes > 1:
        radl_base = utilities.create_basic_radl(radl)
    else:
        radl_base = radl

    # Set availability zone in RADL if necessary
    cloud_info = opa_client.get_cloud(cloud)
    if 'availability_zones' in cloud_info:
        availability_zones = cloud_info['availability_zones']
        if availability_zones:
            shuffle(availability_zones)
            logger.info('Using availability zone %s', availability_zones[0])
            radl_base = utilities.set_availability_zone(radl_base, availability_zones[0])

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
        if infra_status_new == 'deletion-requested' or infra_status_new == 'deleted':
            logger.info('Deletion requested of infrastructure, aborting deployment')
            return None

        # Create infrastructure
        (infrastructure_id, msg) = client.create(radl_base, int(CONFIG.get('timeouts', 'creation')))

        if infrastructure_id:
            logger.info('Created infrastructure on cloud %s with IM id %s and waiting for it to be configured', cloud, infrastructure_id)
            db.deployment_update_status_with_retries(unique_id, None, cloud, infrastructure_id)

            time_created = time.time()
            count_unconfigured = 0
            state_previous = None

            fnodes_to_be_replaced = 0
            wnodes_to_be_replaced = 0
            initial_step_complete = False
            multi_node_deletions = 0

            # Wait for infrastructure to enter the configured state
            while True:
                # Check if we should stop
                (im_infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new == 'deletion-requested':
                    logger.info('Deletion requested of infrastructure, aborting deployment')
                    return None
                elif infra_status_new == 'deleted':
                    logger.info('Deletion requested of infrastructure, aborting deployment, already deleted')
                    return None

                # Don't spend too long trying to create infrastructure, give up eventually
                if time.time() - time_begin > int(CONFIG.get('timeouts', 'total')):
                    logger.info('Giving up, total time waiting is too long, so will destroy infrastructure with IM id %s', infrastructure_id)
                    destroy.destroy(client, infrastructure_id)
                    return None

                # Get the current overall state & states of all VMs in the infrastructure
                time.sleep(int(CONFIG.get('polling', 'duration')))
                (states, msg) = client.getstates(infrastructure_id, int(CONFIG.get('timeouts', 'status')))

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
                    logger.info('State is configured, NumNodesWanted=%d, NumNodesHave=%d, InitialStepComplete=%d', num_nodes, have_nodes, initial_step_complete)

                    # The final configured state
                    if num_nodes == 1 or (num_nodes > 1 and initial_step_complete):
                        logger.info('Successfully configured infrastructure on cloud %s, took %d secs', cloud, time.time() - time_begin_this_cloud)
                        success = True
                        return infrastructure_id

                    # Configured state for initial step of multi-node infrastructure
                    if num_nodes > 1 and have_nodes == num_nodes and not initial_step_complete:
                        logger.info('Successfully configured basic infrastructure on cloud %s, will now apply final configuration', cloud)

                        initial_step_complete = True

                        radl_final = ''
                        for line in radl.split('\n'):
                            if line.startswith('deploy'):
                                line = ''
                            radl_final += '%s\n' % line
                        (exit_code, msg) = client.reconfigure_new(infrastructure_id, radl_final, int(CONFIG.get('timeouts', 'reconfigure')))

                    # Configured state but some nodes failed and were deleted
                    if num_nodes > 1 and have_nodes < num_nodes and not initial_step_complete:
                        logger.info('Infrastructure is now in the configured state but need to re-create failed VMs')

                        if fnodes_to_be_replaced > 0:
                            logger.info('Creating %d fnodes', fnodes_to_be_replaced)
                            radl_new = ''
                            for line in radl_base.split('\n'):
                                if line.startswith('deploy wnode'):
                                    line = ''
                                if line.startswith('deploy fnode'):
                                    line = 'deploy fnode %d\n' % fnodes_to_be_replaced
                                radl_new += '%s\n' % line
                            fnodes_to_be_replaced = 0
                            (exit_code, msg) = client.add_resource(infrastructure_id, radl_new, 120)

                        if wnodes_to_be_replaced > 0:
                            logger.info('Creating %d wnodes', wnodes_to_be_replaced)
                            radl_new = ''
                            for line in radl_base.split('\n'):
                                if line.startswith('deploy fnode'):
                                    line = ''
                                if line.startswith('deploy wnode'):
                                    line = 'deploy wnode %d\n' % wnodes_to_be_replaced
                                radl_new += '%s\n' % line
                            wnodes_to_be_replaced = 0
                            (exit_code, msg) = client.add_resource(infrastructure_id, radl_new, 120)

                # Destroy infrastructure which is taking too long to enter the configured state
                if time.time() - time_created > int(CONFIG.get('timeouts', 'configured')):
                    logger.warning('Waiting too long for infrastructure to be configured, so destroying')
                    opa_client.set_status(cloud, 'configuration-too-long')
                    destroy.destroy(client, infrastructure_id)
                    break

                # Destroy infrastructure which is taking too long to enter the running state
                if time.time() - time_created > int(CONFIG.get('timeouts', 'notrunning')) and state != 'running' and state != 'unconfigured' and num_nodes == 1:
                    logger.warning('Waiting too long for infrastructure to enter the running state, so destroying')
                    opa_client.set_status(cloud, 'pending-too-long')
                    destroy.destroy(client, infrastructure_id)
                    break

                # FIXME: This factor of 3 is a hack
                if time.time() - time_created > 3*int(CONFIG.get('timeouts', 'notrunning')) and state != 'running' and state != 'unconfigured' and num_nodes > 1:
                    logger.warning('Waiting too long for infrastructure to enter the running state, so destroying')
                    opa_client.set_status(cloud, 'pending-too-long')
                    destroy.destroy(client, infrastructure_id)
                    break

                # Destroy infrastructure for which deployment failed
                if state == 'failed':
                    if num_nodes > 1:
                        logger.info('Infrastructure creation failed for some VMs on cloud %s, so deleting these (run %d)', cloud, multi_node_deletions)
                        multi_node_deletions += 1
                        failed_vms = 0
                        for vm_id in states['state']['vm_states']:
                            if states['state']['vm_states'][vm_id] == 'failed':
                                logger.info('Deleting VM with id %d', int(vm_id))
                                failed_vms += 1

                                # Determine what type of node (fnode or wnode)
                                (exit_code, vm_info) = client.get_vm_info(infrastructure_id,
                                                                          int(vm_id),
                                                                          int(CONFIG.get('timeouts', 'deletion')))
                                # FIXME - is found_vm really needed?
                                found_vm = False
                                for info in vm_info['radl']:
                                    if 'state' in info and 'id' in info:
                                        found_vm = True
                                        if 'fnode' in info['id']:
                                            fnodes_to_be_replaced += 1
                                        else:
                                            wnodes_to_be_replaced += 1

                                if not found_vm:
                                    logger.warn('Unable to determine type of VM')

                                # Delete the VM
                                (exit_code, msg_remove) = client.remove_resource(infrastructure_id,
                                                                                 int(vm_id),
                                                                                 int(CONFIG.get('timeouts', 'deletion')))

                        logger.info('Deleted %d failed VMs from infrastructure', failed_vms)

                        # Check if we have deleted all VMs: in this case IM will return 'unknown' as the status
                        # so it's best to just start again
                        if failed_vms == num_nodes:
                            logger.warning('All VMs failed and deleted, so destroying infrastructure')
                            opa_client.set_status(cloud, state)
                            destroy.destroy(client, infrastructure_id)
                            break

                        continue

                    else:
                        logger.warning('Infrastructure creation failed on cloud %s, so destroying', cloud)
                        opa_client.set_status(cloud, state)
                        destroy.destroy(client, infrastructure_id)
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
                        client.reconfigure(infrastructure_id, int(CONFIG.get('timeouts', 'reconfigure')))
                    else:
                        logger.warning('Infrastructure has been unconfigured too many times, so destroying after writing contmsg to a file')
                        opa_client.set_status(cloud, state)
                        try:
                            with open(file_unconf, 'w') as unconf:
                                unconf.write(contmsg)
                        except Exception as error:
                            logger.warning('Unable to write contmsg to file')
                        destroy.destroy(client, infrastructure_id)
                        break
        else:
            logger.warning('Deployment failure on cloud %s with id %s with msg="%s"', cloud, infrastructure_id, msg)
            if msg == 'timedout':
                logger.warning('Infrastructure creation failed due to a timeout')
                opa_client.set_status(cloud, 'creation-timeout')
            else:
                file_failed = '%s/failed-%s-%d.txt' % (CONFIG.get('logs', 'contmsg'), unique_id, time.time())
                opa_client.set_status(cloud, 'creation-failed')
                logger.warning('Infrastructure creation failed, writing stdout/err to file "%s"', file_failed)
                try:
                    with open(file_failed, 'w') as failed:
                        failed.write(msg)
                except Exception as error:
                    logger.warning('Unable to write contmsg to file')

    return None

