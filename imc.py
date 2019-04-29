#!/usr/bin/python

from __future__ import print_function
import base64
import os
import sys
import re
from string import Template
import json
import time
from random import shuffle
import logging
import ConfigParser
import paramiko

import database
import imclient
import opaclient
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
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

def destroy(client, infrastructure_id, cloud):
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
        else:
            logger.warning('Unable to destroy infrastructure with id "%s" on cloud "%s", attempt %d, due to "%s"', infrastructure_id, cloud, count, msg)
        count += 1
        delay = delay*delay_factor
        time.sleep(int(count + delay))
    if not destroyed:
        logger.critical('Unable to destroy infrastructure with id "%s" on cloud "%s" due to "%s"', infrastructure_id, cloud, msg)
    return destroyed

def check_ansible_node(ip_addr, username):
    """
    Check existing Ansible node
    """
    try:
        k = paramiko.RSAKey.from_private_key_file(CONFIG.get('ansible', 'private_key'))
    except Exception as ex:
        logger.crit('Unable to open private key file for Ansible due to: %s', ex)
        return False

    success = False
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip_addr, username=username, pkey=k)
        stdin, stdout, stderr = client.exec_command('pwd')
        client.close()
        success = True
    except Exception as ex:
        logger.info('Unable to execute command on Ansible node with ip %s', ip_addr)
    return success

def delete_ansible_node(cloud, db):
    """
    Delete an Ansible node for the specified cloud
    """
    # Get details about the node
    (infrastructure_id, public_ip, username, timestamp) = db.get_ansible_node(cloud)

    if infrastructure_id is None:
        logger.critical('[delete_ansible_node] Unable to get infrastructure id for Ansible node in cloud %s', cloud)
        return False

    logger.info('[delete_ansible_node] About to delete Ansible node from clouds %s with infrastructure id %s', cloud, infrastructure_id)

    #  Get a token if necessary
    token = tokens.get_token(cloud, db, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])

    # Destroy infrastructure
    im_auth = utilities.create_im_auth(cloud, token, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return False
    (return_code, msg) = client.destroy(infrastructure_id, int(CONFIG.get('timeouts', 'deletion')))
    if return_code != 0:
        logger.critical('Unable to destroy Ansible node infrastructure with id "%s" on cloud "%s" due to "%s"', infrastructure_id, cloud, msg)

    # Delete from the DB
    db.delete_ansible_node(cloud)

    return True

def setup_ansible_node(cloud, db):
    """
    Find or create an Ansible node inside the specified cloud
    """

    # Check if there is a static Ansible node
    (ip_addr, username) = get_static_ansible_node(cloud)

    if ip_addr is not None and username is not None:
        logger.info('Found static Ansible node with ip_addr %s on cloud %s', ip_addr, cloud)
        status = check_ansible_node(ip_addr, username)
        if status:
            logger.info('Successfully tested Ansible node with ip %s on cloud %s', ip_addr, cloud)
            return (ip_addr, username)
        else:
            # If we find a static Ansible node and it doesn't work we will not try to create a
            # dynamic one as there must be a reason why a static node was created
            logger.critical('Ansible node with ip %s on cloud %s not accessible', ip_addr, cloud)
            return (None, None)

    logger.info('No functional static Ansible node found for cloud %s', cloud)
    return (None, None)

    # Check if there is a dynamic Ansible node
    (ip_addr, username) = get_dynamic_ansible_node(cloud, db)

    if ip_addr is not None and username is not None:
        logger.info('Found existing dynamic Ansible node with ip %s on cloud %s', ip_addr, cloud)
        status = check_ansible_node(ip_addr, username)
        if status:
            logger.info('Successfully tested Ansible node with ip %s on cloud %s', ip_addr, cloud)
            return (ip_addr, username)
        else:
            logger.info('Ansible node with ip %s on cloud %s not accessible, so deleting', ip_addr, cloud)
            delete_ansible_node(cloud, db)
    logger.info('No functional dynamic Ansible node found for cloud %s', cloud)

    # Try to create a dynamic Ansible node
    infrastructure_id = deploy_ansible_node(cloud, db)

    if infrastructure_id is None:
        logger.critical('Unable to create Ansible node on cloud "%s"', cloud)
        return (None, None)

    # Get the public IP
    ip_addr = get_public_ip(infrastructure_id)

    if ip_addr is None:
        logger.critical('Newly deployed Ansible node has no public IP')
        return (None, None)

    # Set DB
    db.set_ansible_node(cloud, infrastructure_id, ip_addr, 'cloudadm')

    return (ip_addr, 'cloudadm')

def get_dynamic_ansible_node(cloud, db):
    """
    Check if the given cloud has a dynamic Ansible node and return its details if it does
    """
    (infrastructure_id, public_ip, username, timestamp) = db.get_ansible_node(cloud)
    return (public_ip, username)

def get_static_ansible_node(cloud):
    """
    Check if the given cloud has a static Ansible node and return it's details if it does
    """
    try:
        with open('%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR']) as file:
            data = json.load(file)
    except Exception as ex:
        logger.critical('Unable to open file containing static Ansible nodes due to: %s', ex)
        return (None, None)


    if 'ansible' in data:
        if cloud in data['ansible']:
            return (data['ansible'][cloud]['public_ip'], data['ansible'][cloud]['username'])

    return (None, None)

def get_public_ip(infrastructure_id):
    """
    Get the public IP of infrastructure
    """
    public_ip = None

    # Setup Infrastructure Manager client
    im_auth = utilities.create_im_auth(cloud, None, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return None

    # Get data associated with the Infra ID & find the public IP
    (state, msg) = client.getdata(infrastructure_id, 60)
    m = re.search(r'net_interface.0.ip = \'([\.\d]+)\'', state['data'])
    if m:
        public_ip = m.group(1)

    return public_ip

def deploy_ansible_node(cloud, db):
    """
    Deploy an Ansible node with public IP address
    """
    logger.info('Deploying Ansible node for cloud "%s"', cloud)

    # Open RADL template file
    try:
        with open(CONFIG.get('ansible', 'template')) as data:
            radl_t = Template(data.read())
    except IOError:
        logger.critical('Unable to open RADL template for Ansible node from file "%s"', CONFIG.get('ansible', 'template'))
        return None

    # Generate requirements for the Ansible node
    requirements = {}
    requirements['resources'] = {}
    requirements['resources']['cores'] = int(CONFIG.get('ansible', 'cores'))
    requirements['resources']['memory'] = int(CONFIG.get('ansible', 'memory'))

    requirements['image'] = {}
    requirements['image']['architecture'] = CONFIG.get('ansible', 'architecture')
    requirements['image']['distribution'] = CONFIG.get('ansible', 'distribution')
    requirements['image']['type'] = CONFIG.get('ansible', 'type')
    requirements['image']['version'] = CONFIG.get('ansible', 'version')

    # Generate JSON to be given to Open Policy Agent
    userdata = {'requirements':requirements, 'preferences':{}}

    # Setup Open Policy Agent client
    opa_client = opaclient.OPAClient(url=CONFIG.get('opa', 'url'), timeout=int(CONFIG.get('opa', 'timeout')))

    # Get the image & flavour
    image = opa_client.get_image(userdata, cloud)
    flavour = opa_client.get_flavour(userdata, cloud)

    if image is None:
        logger.critical('Unable to deploy Ansible node because no acceptable image is available')
        return None

    if flavour is None:
        logger.critical('Unable to deploy Ansible node because no acceptable flavour is available')
        return None

    logger.info('Using image "%s" and flavour "%s" to deploy Ansible node', image, flavour)

    # Get the private & public keys
    try:
        with open(CONFIG.get('ansible', 'private_key')) as data:
            private_key = data.read()
    except IOError:
        logger.critical('Unable to open private key for Ansible node from file "%s"', filename)
        return None

    try:
        with open(CONFIG.get('ansible', 'public_key')) as data:
            public_key = data.read()
    except IOError:
        logger.critical('Unable to open public key for Ansible node from file "%s"', filename)
        return None

    # Create complete RADL content
    try:
        radl = radl_t.substitute(instance=flavour,
                                 image=image,
                                 private_key=private_key,
                                 public_key=public_key,
                                 cloud=cloud)
    except Exception as e:
        logger.critical('Error creating RADL from template for Ansible node due to %s', e)
        return None

    time_begin = time.time()

    # Deploy infrastructure
    infra_id = deploy(radl, cloud, time_begin, None, db)

    return infra_id

def deploy_job(db, radl_contents, requirements, preferences, unique_id, dryrun):
    """
    Find an appropriate cloud to deploy infrastructure
    """
    # Count number of instances
    instances = 0
    for line in radl_contents.split('\n'):
        m = re.search(r'deploy.*(\d+)', line)
        if m:
            instances += int(m.group(1))
    logger.info('Found %d instances to deploy', instances)
    requirements['resources']['instances'] = instances

    # Generate JSON to be given to Open Policy Agent
    userdata = {'requirements':requirements, 'preferences':preferences}

    # Setup Open Policy Agent client
    opa_client = opaclient.OPAClient(url=CONFIG.get('opa', 'url'), timeout=int(CONFIG.get('opa', 'timeout')))

    # Get list of clouds meeting the specified requirements
    clouds = opa_client.get_clouds(userdata)
    logger.info('Suitable clouds = [%s]', ','.join(clouds))

    if not clouds:
        logger.critical('No clouds exist which meet the requested requirements')
        return False

    # Update dynamic information about each cloud if necessary

    # Shuffle list of clouds
    shuffle(clouds)

    # Rank clouds as needed
    clouds_ranked = opa_client.get_ranked_clouds(userdata, clouds)
    clouds_ranked_list = []
    for item in sorted(clouds_ranked, key=lambda k: k['weight'], reverse=True):
        clouds_ranked_list.append(item['site'])
    logger.info('Ranked clouds = [%s]', ','.join(clouds_ranked_list))

    # Check if we should stop
    (im_infra_id_new, infra_status_new, cloud_new) = db.deployment_get_im_infra_id(unique_id)
    if infra_status_new == 'deleting':
        logger.info('Deletion requested of infrastructure with our id "%s", aborting deployment', unique_id)
        return False

    # Update status
    db.deployment_update_status_with_retries(unique_id, 'creating')

    # Try to create infrastructure, exiting on the first successful attempt
    time_begin = time.time()
    success = False

    for item in sorted(clouds_ranked, key=lambda k: k['weight'], reverse=True):
        infra_id = None
        cloud = item['site']
        image = opa_client.get_image(userdata, cloud)
        flavour = opa_client.get_flavour(userdata, cloud)
        logger.info('Attempting to deploy on cloud "%s" with image "%s" and flavour "%s"', cloud, image, flavour)

        # If no flavour meets the requirements we should skip the current cloud
        if flavour is None:
            logger.info('Skipping because no flavour could be determined')
            continue

        # If no image meets the requirements we should skip the current cloud
        if image is None:
            logger.info('Skipping because no image could be determined')
            continue

        if dryrun:
            continue

        # Setup Ansible node if necessary
        if requirements['resources']['instances'] > 1 and 'Google' not in cloud:
            (ip_addr, username) = setup_ansible_node(cloud, db)
            if ip_addr is None or username is None:
                logger.critical('Unable to find existing or create an Ansible node in cloud %s because ip=%s,username=%s', cloud, ip_addr, username)
                continue
            logger.info('Ansible node in cloud %s available, now will deploy infrastructure for the job', cloud)
        else:
            logger.info('Ansible node not required')
            ip_addr = None
            username = None

        # Get the private key
        try:
            with open(CONFIG.get('ansible', 'private_key')) as data:
                private_key = data.read()
        except IOError:
            logger.critical('Unable to open private key for Ansible node from file "%s"', filename)
            return False


        # Create complete RADL content
        try:
            radl = Template(radl_contents).substitute(instance=flavour,
                                                      image=image,
                                                      cloud=cloud,
                                                      ansible_ip=ip_addr,
                                                      ansible_username=username,
                                                      ansible_private_key=private_key)
        except Exception as ex:
            logger.critical('Error creating RADL from template due to %s', ex)
            return False

        # Check if we should stop
        (im_infra_id_new, infra_status_new, cloud_new) = db.deployment_get_im_infra_id(unique_id)
        if infra_status_new == 'deleting':
            logger.info('Deletion requested of infrastructure with our id "%s", aborting deployment', unique_id)
            return False

        # Deploy infrastructure
        try:
            infra_id = deploy(radl, cloud, time_begin, unique_id, db, int(requirements['resources']['instances']))
        except Exception as error:
            logger.critical('Deployment error for id %s, this is a bug: %s', unique_id, error)

        if infra_id is not None:
            success = True
            if unique_id is not None:
                # Final check if we should delete the infrastructure
                (im_infra_id_new, infra_status_new, cloud_new) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new == 'deleting':
                    logger.info('Deletion requested of infrastructure with our id "%s", aborting deployment', unique_id)

                    token = tokens.get_token(cloud, db, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])

                    im_auth = utilities.create_im_auth(cloud, token, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
                    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
                    (status, msg) = client.getauth()
                    if status != 0:
                        logger.critical('Error reading IM auth file: %s', msg)
                        return False

                    destroy(client, infra_id, cloud)
                    return False
                else:
                    db.deployment_update_status_with_retries(unique_id, 'configured', cloud, infra_id)
            break

    if unique_id is not None and infra_id is None:
        db.deployment_update_status_with_retries(unique_id, 'failed', 'none', 'none')
    return success

def deploy(radl, cloud, time_begin, unique_id, db, num_nodes=1):
    """
    Deploy infrastructure from a specified RADL file
    """
    # Check & get auth token if necessary
    token = tokens.get_token(cloud, db, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])

    # Setup Open Policy Agent client
    opa_client = opaclient.OPAClient(url=CONFIG.get('opa', 'url'),
                                     timeout=int(CONFIG.get('opa', 'timeout')))

    # Setup Infrastructure Manager client
    im_auth = utilities.create_im_auth(cloud, token, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
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

    # Retry loop
    retries_per_cloud = int(CONFIG.get('deployment', 'retries'))
    retry = 0
    success = False
    while retry < retries_per_cloud + 1 and success is not True:
        if retry > 0:
            time.sleep(int(CONFIG.get('polling', 'duration')))
        logger.info('Deployment attempt %d of %d', retry+1, retries_per_cloud+1)
        retry += 1

        # Check if we should stop
        (im_infra_id_new, infra_status_new, cloud_new) = db.deployment_get_im_infra_id(unique_id)
        if infra_status_new == 'deleting':
            logger.info('Deletion requested of infrastructure with our id "%s", aborting deployment', unique_id)
            return None

        # Create infrastructure
        start = time.time()
        (infrastructure_id, msg) = client.create(radl_base, int(CONFIG.get('timeouts', 'creation')))
        duration = time.time() - start
        logger.info('Duration of create request %d s on cloud %s', duration, cloud)

        if infrastructure_id is not None:
            logger.info('Created infrastructure with id "%s" on cloud "%s" for id "%s" and waiting for it to be configured', infrastructure_id, cloud, unique_id)
            db.deployment_update_status_with_retries(unique_id, None, cloud, infrastructure_id)

            time_created = time.time()
            count_unconfigured = 0
            state_previous = None

            fnodes_to_be_replaced = 0
            wnodes_to_be_replaced = 0
            have_nodes = -1
            initial_step_complete = False
            multi_node_deletions = 0

            # Wait for infrastructure to enter the configured state
            while True:
                # Check if we should stop
                (im_infra_id_new, infra_status_new, cloud_new) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new == 'deleting':
                    logger.info('Deletion requested of infrastructure with our id "%s", aborting deployment', unique_id)
                    destroy(client, infrastructure_id, cloud)
                    return None

                # Don't spend too long trying to create infrastructure, give up eventually
                if time.time() - time_begin > int(CONFIG.get('timeouts', 'total')):
                    logger.info('Giving up, total time waiting is too long, so will destroy infrastructure with id "%s"', infrastructure_id)
                    destroy(client, infrastructure_id, cloud)
                    return None

                time.sleep(int(CONFIG.get('polling', 'duration')))
                (states, msg) = client.getstates(infrastructure_id, int(CONFIG.get('timeouts', 'status')))

                # If state is not known, wait
                if states is None:
                    logger.info('State is not known for infrastructure with id "%s" on cloud "%s"', infrastructure_id, cloud)
                    continue

                # Overall state of infrastructure
                state = states['state']['state']
                have_nodes = len(states['state']['vm_states'])

                # Log a change in state
                if state != state_previous:
                    logger.info('Infrastructure with our id "%s" and IM id "%s" is in state %s', unique_id, infrastructure_id, state)
                    state_previous = state

                # Handle final configured state
                if state == 'configured' and (num_nodes == 1 or (num_nodes > 1 and initial_step_complete)):
                    logger.info('Successfully configured infrastructure with our id "%s" on cloud "%s"', unique_id, cloud)
                    success = True
                    return infrastructure_id

                # Handle configured state for initial step of multi-node infrastructure
                if state == 'configured' and num_nodes > 1 and have_nodes == num_nodes and not initial_step_complete:
                    logger.info('Successfully configured basic infrastructure with our id "%s" on cloud "%s", will now apply final configuration', unique_id, cloud)
                    initial_step_complete = True

                    radl_final = ''
                    for line in radl.split('\n'):
                        if line.startswith('deploy'):
                            line = ''
                        radl_final += '%s\n' % line
                    (exit_code, msg) = client.reconfigure_new(infrastructure_id, radl_final, int(CONFIG.get('timeouts', 'reconfigure')))

                # Handle configured state but some nodes failed and were deleted
                if state == 'configured' and num_nodes > 1 and have_nodes < num_nodes and not initial_step_complete:
                    logger.info('Infrastructure with our id "%s" is now in the configured state but need to re-create failed VMs', unique_id)
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
                    destroy(client, infrastructure_id, cloud)
                    break

                # Destroy infrastructure which is taking too long to enter the running state
                if time.time() - time_created > int(CONFIG.get('timeouts', 'notrunning')) and state != 'running' and state != 'unconfigured' and num_nodes == 1:
                    logger.warning('Waiting too long for infrastructure to enter the running state, so destroying')
                    opa_client.set_status(cloud, 'pending-too-long')
                    destroy(client, infrastructure_id, cloud)
                    break

                # This factor of 3 is a hack, need to fix!
                if time.time() - time_created > 3*int(CONFIG.get('timeouts', 'notrunning')) and state != 'running' and state != 'unconfigured' and num_nodes > 1:
                    logger.warning('Waiting too long for infrastructure to enter the running state, so destroying')
                    opa_client.set_status(cloud, 'pending-too-long')
                    destroy(client, infrastructure_id, cloud)
                    break

                # Destroy infrastructure for which deployment failed
                if state == 'failed':
                    if num_nodes > 1:
                        logger.info('Infrastructure creation failed for some VMs, so deleting these (run %d)', multi_node_deletions)
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
                                for info in vm_info['radl']:
                                    if 'state' in info:
                                        if 'fnode' in info['id']:
                                            fnodes_to_be_replaced += 1
                                        else:
                                            wnodes_to_be_replaced += 1

                                # Delete the VM
                                (exit_code, msg_remove) = client.remove_resource(infrastructure_id,
                                                                                 int(vm_id),
                                                                                 int(CONFIG.get('timeouts', 'deletion')))

                        logger.info('Deleted %d failed VMs from infrastructure with our id %s', failed_vms, unique_id)

                        # Check if we have deleted all VMs: in this case IM will return 'unknown' as the status
                        # so it's best to just start again
                        if failed_vms == num_nodes:
                            logger.warning('All VMs failed and deleted, so destroying infrastructure for our id %s', unique_id)
                            opa_client.set_status(cloud, state)
                            destroy(client, infrastructure_id, cloud)
                            break
                        continue

                    else:
                        logger.warning('Infrastructure creation failed, so destroying')
                        opa_client.set_status(cloud, state)
                        destroy(client, infrastructure_id, cloud)
                        break

                # Handle unconfigured infrastructure
                if state == 'unconfigured':
                    count_unconfigured += 1
                    file_unconf = '/tmp/contmsg-%s-%d.txt' % (unique_id, time.time())
                    contmsg = client.getcontmsg(infrastructure_id, int(CONFIG.get('timeouts', 'deletion')))
                    if count_unconfigured < 2:
                        logger.warning('Infrastructure is unconfigured, will try reconfiguring once after writing contmsg to a file')
                        with open(file_unconf, 'w') as unconf:
                            unconf.write(contmsg)
                        client.reconfigure(infrastructure_id, int(CONFIG.get('timeouts', 'reconfigure')))
                    else:
                        logger.warning('Infrastructure has been unconfigured too many times, so destroying after writing contmsg to a file')
                        opa_client.set_status(cloud, state)
                        with open(file_unconf, 'w') as unconf:
                            unconf.write(contmsg)
                        destroy(client, infrastructure_id, cloud)
                        break
        else:
            logger.warning('Deployment failure on cloud "%s" for infrastructure with id "%s" with msg="%s"', cloud, infrastructure_id, msg)
            if msg == 'timedout':
                logger.warning('Infrastructure creation failed due to a timeout')
                opa_client.set_status(cloud, 'creation-timeout')
            else:
                file_failed = '/tmp/failed-%s-%d.txt' % (unique_id, time.time())
                opa_client.set_status(cloud, 'creation-failed')
                logger.warning('Infrastructure creation failed, writing stdout/err to file "%s"', file_failed)
                with open(file_failed, 'w') as failed:
                    failed.write(msg)

    return None

def infrastructure_delete(unique_id):
    """
    Delete the infrastructure with the specified id (wrapper)
    """
    try:
        imc_delete(unique_id)
    except Exception as ex:
        logger.critical('Exception deleting infrastructure: "%s"' % ex)
    return

def imc_delete(unique_id):
    """
    Delete the infrastructure with the specified id
    """
    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))
    db.connect()
    logger.info('Deleting infrastructure "%s"', unique_id)

    (im_infra_id, infra_status, cloud) = db.deployment_get_im_infra_id(unique_id)

    if im_infra_id is not None and cloud is not None:
        match_obj_name = re.match(r'\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b', im_infra_id)
        if match_obj_name:
            logger.info('Deleting IM infrastructure with id "%s"', im_infra_id)
            # Check & get auth token if necessary
            token = tokens.get_token(cloud, db, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])

            # Setup Infrastructure Manager client
            im_auth = utilities.create_im_auth(cloud, token, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
            client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
            (status, msg) = client.getauth()
            if status != 0:
                logger.critical('Error reading IM auth file: %s', msg)
                db.close()
                return 1

            destroyed = destroy(client, im_infra_id, cloud)

            if destroyed:
                db.deployment_update_status_with_retries(unique_id, 'deleted')
                logger.info('Destroyed infrastructure "%s" with IM infrastructure id "%s"', unique_id, im_infra_id)
            else:
                logger.info('Unable to destroy infrastructure "%s" with IM infrastructure id "%s"', unique_id, im_infra_id)
    else:
        logger.info('No need to destroy infrastructure because IM infrastructure id is "%s" and cloud is "%s"', im_infra_id, cloud)
        db.deployment_update_status_with_retries(unique_id, 'deleted')
    db.close()
    return 0

def infrastructure_status(unique_id):
    """
    Return the status of the infrastructure from the specified id
    """
    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))
    db.connect()
    (im_infra_id, status, cloud) = db.deployment_get_im_infra_id(unique_id)
    db.close()
    return (im_infra_id, status, cloud)

def infrastructure_deploy(inputj, unique_id):
    """
    Deploy infrastructure given a JSON specification and id (wrapper)
    """
    try:
        imc_deploy(inputj, unique_id)
    except Exception as ex:
        logger.critical('Exception deploying infrastructure: "%s"', ex)
    return

def imc_deploy(inputj, unique_id):
    """
    Deploy infrastructure given a JSON specification and id
    """
    dryrun = False
    logger.info('Deploying infrastructure with id %s', unique_id)

    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))
    db.connect()

    # Generate requirements & preferences
    if 'preferences' in inputj:
        preferences_new = {}
        # Generate list of weighted regions if necessary
        if 'regions' in inputj['preferences']:
            preferences_new['regions'] = {}
            for i in range(0, len(inputj['preferences']['regions'])):
                preferences_new['regions'][inputj['preferences']['regions'][i]] = len(inputj['preferences']['regions']) - i
        # Generate list of weighted sites if necessary
        if 'sites' in inputj['preferences']:
            preferences_new['sites'] = {}
            for i in range(0, len(inputj['preferences']['sites'])):
                preferences_new['sites'][inputj['preferences']['sites'][i]] = len(inputj['preferences']['sites']) - i
        inputj['preferences'] = preferences_new

    if 'requirements' in inputj:
        requirements = inputj['requirements']
        preferences = inputj['preferences']

    if 'radl' in inputj:
        radl_contents = base64.b64decode(inputj['radl'])

    # Attempt to deploy infrastructure
    success = deploy_job(db, radl_contents, requirements, preferences, unique_id, dryrun)

    if not success:
        db.deployment_update_status_with_retries(unique_id, 'unable')

    db.close()

    if not success:
        logger.critical('Unable to deploy infrastructure on any cloud')
        return 1

    return 0
