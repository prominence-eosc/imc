#!/usr/bin/python

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

def deploy_job(db, radl_contents, requirements, preferences, unique_id, dryrun):
    """
    Find an appropriate cloud to deploy infrastructure
    """
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
    if infra_status_new == 'deletion-requested':
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
 
        # Stop here if necessary
        if dryrun:
            continue

        # Setup Ansible node if necessary
        if requirements['resources']['instances'] > 1:
            (ip_addr, username) = ansible.setup_ansible_node(cloud, db)
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
        (im_infra_id_new, infra_status_new, cloud_new) = db.deployment_get_im_infra_id(unique_id)
        if infra_status_new == 'deletion-requested':
            logger.info('Deletion requested of infrastructure with our id "%s", aborting deployment', unique_id)
            return False

        # Deploy infrastructure
        try:
            infra_id = deploy.deploy(radl, cloud, time_begin, unique_id, db, int(requirements['resources']['instances']))
        except Exception as error:
            logger.critical('Deployment error for id %s, this is a bug: %s', unique_id, error)

        if infra_id is not None:
            success = True
            if unique_id is not None:
                # Final check if we should delete the infrastructure
                (im_infra_id_new, infra_status_new, cloud_new) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new == 'deletion-requested':
                    logger.info('Deletion requested of infrastructure with our id "%s", aborting deployment', unique_id)

                    token = tokens.get_token(cloud, db, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])

                    im_auth = utilities.create_im_auth(cloud, token, '%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
                    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
                    (status, msg) = client.getauth()
                    if status != 0:
                        logger.critical('Error reading IM auth file: %s', msg)
                        return False

                    destroyed = destroy.destroy(client, infra_id, cloud)

                    if destroyed:
                        db.deployment_update_status_with_retries(unique_id, 'deleted')
                        logger.info('Destroyed infrastructure "%s" with IM infrastructure id "%s"', unique_id, infra_id)
                    else:
                        db.deployment_update_status_with_retries(unique_id, 'deletion-failed')
                        logger.critical('Unable to destroy infrastructure "%s" with IM infrastructure id "%s"', unique_id, infra_id)

                    return False
                else:
                    db.deployment_update_status_with_retries(unique_id, 'configured', cloud, infra_id)
            break

    if unique_id is not None and infra_id is None:
        db.deployment_update_status_with_retries(unique_id, 'failed', 'none', 'none')
    return success

def delete(unique_id):
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
    logger.info('Obtained IM id %s and cloud %s and status %s for infrastructure with id %s', im_infra_id, cloud, infra_status, unique_id)

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

            try:
                destroyed = destroy.destroy(client, im_infra_id, cloud)
            except Exception as error:
                logger.critical('Deletion bug for id %s, error: %s', unique_id, error)

            if destroyed:
                db.deployment_update_status_with_retries(unique_id, 'deleted')
                logger.info('Destroyed infrastructure "%s" with IM infrastructure id "%s"', unique_id, im_infra_id)
            else:
                db.deployment_update_status_with_retries(unique_id, 'deletion-failed')
                logger.critical('Unable to destroy infrastructure "%s" with IM infrastructure id "%s"', unique_id, im_infra_id)
    else:
        logger.info('No need to destroy infrastructure because IM infrastructure id is "%s" and cloud is "%s"', im_infra_id, cloud)
        db.deployment_update_status_with_retries(unique_id, 'deleted')
    db.close()
    return 0

def auto_deploy(inputj, unique_id):
    """
    Deploy infrastructure given a JSON specification and id
    """
    dryrun = False
    logger.info('Deploying infrastructure with id %s', unique_id)

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

    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))

    success = False
    if db.connect():
        success = deploy_job(db, radl_contents, requirements, preferences, unique_id, dryrun)
        if not success:
            db.deployment_update_status_with_retries(unique_id, 'unable')
    db.close()

    if not success:
        logger.critical('Unable to deploy infrastructure on any cloud')
        return 1

    return 0
