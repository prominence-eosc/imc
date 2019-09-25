from __future__ import print_function
import base64
import os
import sys
import re
from string import Template
import time
from random import shuffle
import logging
import ConfigParser

import ansible
import database
import deploy
import destroy
import imclient
import opaclient
import tokens
import utilities
import cloud_images_flavours
import cloud_quotas
import logger as custom_logger

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

def deploy_job(db, radl_contents, requirements, preferences, unique_id, dryrun):
    """
    Find an appropriate cloud to deploy infrastructure
    """
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': unique_id})

    # Update status as we are now handing deployment of the infrastructure
    db.deployment_update_status_with_retries(unique_id, 'creating')

    # Count number of instances
    instances = 0
    for line in radl_contents.split('\n'):
        m = re.search(r'deploy.*\s(\d+)', line)
        if m:
            instances += int(m.group(1))
    logger.info('Found %d instances to deploy', instances)
    requirements['resources']['instances'] = instances

    # Generate JSON to be given to Open Policy Agent
    userdata = {'requirements':requirements, 'preferences':preferences}

    # Setup Open Policy Agent client
    opa_client = opaclient.OPAClient(url=CONFIG.get('opa', 'url'), timeout=int(CONFIG.get('opa', 'timeout')))

    # Update available clouds & their static info if necessary
    logger.info('Updating static cloud info')
    utilities.update_clouds(opa_client, CONFIG.get('clouds', 'path'))

    # Get full list of cloud info
    clouds_info_list = utilities.create_clouds_list(CONFIG.get('clouds', 'path'))

    # Update cloud images & flavours if necessary
    logger.info('Updating cloud images and flavours if necessary')
    cloud_images_flavours.update_cloud_details(requirements, db, opa_client, clouds_info_list)

    # Update quotas if necessary
    logger.info('Updating cloud quotas if necessary')
    cloud_quotas.set_quotas(requirements, db, opa_client, clouds_info_list)

    # Get list of clouds meeting the specified requirements
    try:
        clouds = opa_client.get_clouds(userdata)
    except Exception as err:
        logger.critical('Unable to get list of clouds due to:', err)
        return False

    logger.info('Suitable clouds = [%s]', ','.join(clouds))

    if not clouds:
        logger.critical('No clouds exist which meet the requested requirements')
        db.deployment_update_status_reason(unique_id, 'NoMatchingResources')
        return False

    # Shuffle list of clouds
    shuffle(clouds)

    # Rank clouds as needed
    try:
        clouds_ranked = opa_client.get_ranked_clouds(userdata, clouds)
    except Exception as err:
        logger.critical('Unable to get list of ranked clouds due to:', err)
        return False

    clouds_ranked_list = []
    for item in sorted(clouds_ranked, key=lambda k: k['weight'], reverse=True):
        clouds_ranked_list.append(item['site'])
    logger.info('Ranked clouds = [%s]', ','.join(clouds_ranked_list))

    # Check if we still have any clouds meeting requirements & preferences
    if not clouds_ranked:
        logger.critical('No suitables clouds after ranking - if we get to this point there must be a bug in the OPA policy')
        db.deployment_update_status_reason(unique_id, 'NoMatchingResources')
        return False

    # Check if we should stop
    (im_infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_im_infra_id(unique_id)
    if infra_status_new == 'deletion-requested' or infra_status_new == 'deleted':
        logger.info('Deletion requested of infrastructure, aborting deployment')
        return False

    # Try to create infrastructure, exiting on the first successful attempt
    time_begin = time.time()
    success = False

    for item in sorted(clouds_ranked, key=lambda k: k['weight'], reverse=True):
        infra_id = None
        cloud = item['site']
        
        try:
            image = opa_client.get_image(userdata, cloud)
        except Exception as err:
            logger.critical('Unable to get image due to:', err)
            return False

        try:
            flavour = opa_client.get_flavour(userdata, cloud)
        except Exception as err:
            logger.critical('Unable to get flavour due to:', err)
            return False

        # If no flavour meets the requirements we should skip the current cloud
        if not flavour:
            logger.info('Skipping because no flavour could be determined')
            continue

        # If no image meets the requirements we should skip the current cloud
        if not image:
            logger.info('Skipping because no image could be determined')
            continue

        logger.info('Attempting to deploy on cloud %s with image %s and flavour %s', cloud, image, flavour)
 
        # Stop here if necessary
        if dryrun:
            continue

        # Setup Ansible node if necessary
        if requirements['resources']['instances'] > 1:
            (ip_addr, username) = ansible.setup_ansible_node(cloud, db)
            if not ip_addr or not username:
                logger.critical('Unable to find existing or create an Ansible node in cloud %s because ip=%s,username=%s', cloud, ip_addr, username)
                continue
            logger.info('Ansible node in cloud %s available, now will deploy infrastructure for the job', cloud)
        else:
            logger.info('Ansible node not required')
            ip_addr = None
            username = None

        # Get the Ansible private key if necessary
        private_key = None
        if ip_addr and username:
            try:
                with open(CONFIG.get('ansible', 'private_key')) as data:
                    private_key = data.read()
            except IOError:
                logger.critical('Unable to open private key for Ansible node from file "%s"', CONFIG.get('ansible', 'private_key'))
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
        (im_infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_im_infra_id(unique_id)
        if infra_status_new == 'deletion-requested' or infra_status_new == 'deleted':
            logger.info('Deletion requested of infrastructure, aborting deployment')
            return False

        # Deploy infrastructure
        try:
            infra_id = deploy.deploy(radl, cloud, time_begin, unique_id, db, int(requirements['resources']['instances']))
        except Exception as error:
            logger.critical('Deployment error, this is a bug: %s', error)
            print(error)

        if infra_id:
            success = True
            if unique_id:
                # Final check if we should delete the infrastructure
                (im_infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new == 'deleted':
                    return False
                elif infra_status_new == 'deletion-requested':
                    logger.info('Deletion requested of infrastructure, aborting deployment')

                    token = tokens.get_token(cloud, db, clouds_info_list)

                    im_auth = utilities.create_im_auth(cloud, token, clouds_info_list)
                    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
                    (status, msg) = client.getauth()
                    if status != 0:
                        logger.critical('Error reading IM auth file: %s', msg)
                        return False

                    destroyed = destroy.destroy(client, infra_id)

                    if destroyed:
                        db.deployment_update_status_with_retries(unique_id, 'deleted')
                        logger.info('Destroyed infrastructure with IM infrastructure id %s', infra_id)
                    else:
                        db.deployment_update_status_with_retries(unique_id, 'deletion-failed')
                        logger.critical('Unable to destroy infrastructure with IM infrastructure id %s', infra_id)

                    return False
                else:
                    db.deployment_update_status_with_retries(unique_id, 'configured', cloud, infra_id)
            break

    if unique_id and not infra_id:
        db.deployment_update_status_with_retries(unique_id, 'failed', 'none', 'none')
        db.deployment_update_status_reason(unique_id, 'DeploymentFailed')
    return success

def delete(unique_id):
    """
    Delete the infrastructure with the specified id
    """
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': unique_id})

    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))
    db.connect()
    logger.info('Deleting infrastructure')

    (im_infra_id, infra_status, cloud, _, _) = db.deployment_get_im_infra_id(unique_id)
    logger.info('Obtained IM id %s and cloud %s and status %s', im_infra_id, cloud, infra_status)

    # Get full list of cloud info
    clouds_info_list = utilities.create_clouds_list(CONFIG.get('clouds', 'path'))

    if im_infra_id and cloud:
        match_obj_name = re.match(r'\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b', im_infra_id)
        if match_obj_name:
            logger.info('Deleting infrastructure with IM id %s', im_infra_id)
            # Check & get auth token if necessary
            token = tokens.get_token(cloud, db, clouds_info_list)

            # Setup Infrastructure Manager client
            im_auth = utilities.create_im_auth(cloud, token, clouds_info_list)
            client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
            (status, msg) = client.getauth()
            if status != 0:
                logger.critical('Error reading IM auth file: %s', msg)
                db.close()
                return 1

            destroyed = destroy.destroy(client, im_infra_id)

            if destroyed:
                db.deployment_update_status_with_retries(unique_id, 'deleted')
                logger.info('Destroyed infrastructure with IM infrastructure id %s', im_infra_id)
            else:
                db.deployment_update_status_with_retries(unique_id, 'deletion-failed')
                logger.critical('Unable to destroy infrastructure with IM infrastructure id %s', im_infra_id)
        else:
            logger.critical('IM infrastructure id %s does not match regex', im_infra_id)
            db.deployment_update_status_with_retries(unique_id, 'deleted')
    else:
        logger.info('No need to destroy infrastructure because IM infrastructure id is %s and cloud is %s', im_infra_id, cloud)
        db.deployment_update_status_with_retries(unique_id, 'deleted')
    db.close()
    return 0

def auto_deploy(inputj, unique_id):
    """
    Deploy infrastructure given a JSON specification and id
    """
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': unique_id})

    dryrun = False
    logger.info('Deploying infrastructure')

    if not inputj:
        logger.warning('No input JSON provided')
        return 1        

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
        try:
            radl_contents = base64.b64decode(inputj['radl'])
        except Exception as err:
            logger.warning('Invalid RADL provided: cannot be decoded')
            return 1

    if 'requirements' not in inputj or 'radl' not in inputj:
        logger.warning('Invalid JSON provided: both requirements and radl must exist')
        return 1

    logger.info('Have job requirements, preferences and RADL, about to connect to the DB')

    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))

    success = False
    if db.connect():
        logger.info('Connected to DB, about to deploy infrastructure for job')
        try:
            success = deploy_job(db, radl_contents, requirements, preferences, unique_id, dryrun)
        except Exception as error:
            print(error)
            logger.critical('deploy_job failed with exception', str(error))
        if not success:
            db.deployment_update_status_with_retries(unique_id, 'unable')
    db.close()

    if not success:
        logger.critical('Unable to deploy infrastructure on any cloud')
        return 1

    return 0
