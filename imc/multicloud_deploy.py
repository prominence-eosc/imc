from __future__ import print_function
import base64
import os
import sys
import re
from string import Template
import time
from random import shuffle
import logging
import configparser
import tempfile

from imc import ansible
from imc import database
from imc import deploy
from imc import destroy
from imc import imclient
from imc import opaclient
from imc import tokens
from imc import utilities
from imc import cloud_images_flavours
from imc import cloud_quotas

# Configuration
CONFIG = utilities.get_config()

# Logging
logger = logging.getLogger(__name__)

def deploy_job(db, unique_id):
    """
    Find an appropriate cloud to deploy infrastructure
    """
    # Update status as we are now handing deployment of the infrastructure
    db.deployment_update_status_with_retries(unique_id, 'creating')

    # Get JSON description & identity from the DB
    (description, identity) = db.deployment_get_json(unique_id)

    # Get RADL
    radl_contents = utilities.get_radl(description)
    if not radl_contents:
        logging.critical('RADL must be provided')
        db.deployment_update_status_with_retries(unique_id, 'unable')
        db.close()
        exit(1)

    # Get requirements & preferences
    (requirements, preferences) = utilities.get_reqs_and_prefs(description)

    # Count number of instances
    instances = utilities.get_num_instances(radl_contents)
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
    cloud_images_flavours.update_cloud_details(requirements, db, identity, opa_client, clouds_info_list)

    # Update quotas if necessary
    logger.info('Updating cloud quotas if necessary')
    cloud_quotas.set_quotas(requirements, db, identity, opa_client, clouds_info_list)

    # Check if clouds are functional
    logger.info('Checking if clouds are functional')
    utilities.update_clouds_status(opa_client, db, identity, clouds_info_list)

    # Get list of clouds meeting the specified requirements
    try:
        clouds = opa_client.get_clouds(userdata)
    except Exception as err:
        logger.critical('Unable to get list of clouds due to %s:', err)
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
 
        # Setup Ansible node if necessary
        if requirements['resources']['instances'] > 1:
            (ip_addr, username) = ansible.setup_ansible_node(cloud, identity, db)
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
            radl = Template(str(radl_contents)).substitute(instance=flavour,
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
        infra_id = deploy.deploy(radl, cloud, time_begin, unique_id, identity, db, int(requirements['resources']['instances']))

        if infra_id:
            success = True
            if unique_id:
                # Final check if we should delete the infrastructure
                (im_infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new == 'deleted':
                    return False
                elif infra_status_new == 'deletion-requested':
                    logger.info('Deletion requested of infrastructure, aborting deployment')
                    destroy.delete(infra_id)
                    return False
                else:
                    db.deployment_update_status_with_retries(unique_id, 'configured', cloud, infra_id)
            break

    if unique_id and not infra_id:
        db.deployment_update_status_with_retries(unique_id, 'failed', 'none', 'none')
        db.deployment_update_status_reason(unique_id, 'DeploymentFailed')
    return success

