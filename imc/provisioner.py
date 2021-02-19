from __future__ import print_function
from string import Template
import time
from random import shuffle
import logging

from imc import ansible
from imc import cloud_deploy
from imc import cloud_utils
from imc import im_utils
from imc import config
from imc import utilities
from imc import cloud_images_flavours
from imc import cloud_quotas
from imc import policies
from imc import tokens

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def deploy_job(db, unique_id):
    """
    Find an appropriate resource to deploy infrastructure
    """
    # Get JSON description & identity from the DB
    (description, identity, identifier) = db.deployment_get_json(unique_id)
    logger.info('Deploying infrastructure %s with identifier %s', unique_id, identifier)

    # Get RADL
    radl_contents = im_utils.get_radl(description)
    if not radl_contents:
        logging.critical('RADL must be provided')
        db.deployment_update_status(unique_id, 'unable')
        return None

    # Get requirements & preferences
    requirements = {}
    preferences = {}
    if 'requirements' in description:
        requirements = description['requirements']
    if 'prefereneces' in description:
        preferences = description['preferences']

    job_want = description['want']

    # Count number of instances
    instances = im_utils.get_num_instances(radl_contents)
    logger.info('Found %d instances to deploy', instances)
    requirements['resources']['instances'] = instances

    # Get full list of cloud info
    logger.info('Getting list of clouds from DB')
    clouds_info_list = cloud_utils.create_clouds_list(db, identity)

    # Setup policy engine
    logger.info('Setting up policies')
    policy = policies.PolicyEngine(clouds_info_list, requirements, preferences, db, identity)

    # Check if deployment could be possible, ignoring current quotas/usage
    logger.info('Checking if job requirements will match any clouds')
    clouds_check = policy.statisfies_requirements(ignore_usage=True)

    if not clouds_check:
        logger.critical('No clouds exist which meet the requested requirements')
        db.deployment_update_status_reason(unique_id, 'NoMatchingResources')
        return None

    # Update quotas if necessary
    # TODO: move this so it's done once per identity, not multiple times
    logger.info('Updating cloud quotas if necessary')
    cloud_quotas.set_quotas(requirements, db, identity, clouds_info_list)

    # Get list of clouds meeting the specified requirements
    clouds = policy.statisfies_requirements()

    logger.info('Suitable resources = [%s]', ','.join(clouds))
    if not clouds:
        logger.critical('No resources exist which meet the requested requirements')
        db.deployment_update_status_reason(unique_id, 'NoMatchingResourcesAvailable')
        return False

    # Shuffle list of clouds
    shuffle(clouds)

    # Rank clouds as needed
    clouds_ranked = policy.rank(clouds)
    logger.info('Ranked clouds = [%s]', ','.join(clouds_ranked))

    # Check if we still have any clouds meeting requirements & preferences
    if not clouds_ranked:
        logger.critical('No suitables clouds after ranking - if we get to this point there must be a bug in the policies')
        db.deployment_update_status_reason(unique_id, 'DeploymentFailed')
        return False

    # Check if we should stop
    (_, infra_status_new, _, _, _) = db.deployment_get_im_infra_id(unique_id)
    if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
        logger.info('Deletion requested of infrastructure, aborting deployment')
        return False

    # Try to create infrastructure, exiting on the first successful attempt
    time_begin = time.time()
    success = False
    reason = None

    for cloud in clouds_ranked:
        infra_id = None

        resource_type = None
        for cloud_info in clouds_info_list:
            if cloud_info['name'] == cloud:
                resource_type = cloud_info['type']

        if resource_type:
            logger.info('Resource %s is of type %s', cloud, resource_type)
        else:
            logger.info('Skipping because no resource type could be determined for resource %s', cloud)
            continue
        
        # Get image
        try:
            (image_name, image_url) = policy.get_image(cloud)
        except Exception as err:
            logger.critical('Unable to get image due to %s', err)
            return False

        # Get flavours
        try:
            flavours = policy.get_flavours(cloud)
            #(flavour, flavour_cpus, flavour_memory, _) = policy.get_flavour(cloud)
        except Exception as err:
            logger.critical('Unable to get flavours due to %s', err)
            return False

        # If no flavour meets the requirements we should skip the current cloud
        if not flavours:
            logger.info('Skipping because no flavour could be determined')
            continue

        # Generate list of flavours of different classs - one class might have no
        # more available hypervisors but another is fine
        new_flavours = [flavours[0]]
        new_flavours_names = [flavours[0][0]]
        old_flavours_names = [flavours[0][0]]
        first_chars = [flavours[0][0][0]]
        for flavour in flavours:
            if flavour in new_flavours:
                continue

            old_flavours_names.append(flavour[0])

            found = False
            for first_char in first_chars:
                if flavour[0].startswith(first_char):
                    found = True

            if not found:
                new_flavours.append(flavour)
                new_flavours_names.append(flavour[0])
                first_chars.append(flavour[0][0])

        flavours = new_flavours
        logger.info('All flavours matching job: %s', ','.join(old_flavours_names))
        logger.info('Flavours matching job: %s', ','.join(new_flavours_names))

        # If no image meets the requirements we should skip the current cloud
        if not image_name:
            logger.info('Skipping because no image could be determined')
            continue

        logger.info('Attempting to deploy on cloud %s', cloud)
 
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

        # Check if we should stop
        (_, infra_status_new, _, _, _) = db.deployment_get_im_infra_id(unique_id)
        if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
            logger.info('Deletion requested of infrastructure, aborting deployment')
            return False

        # Loop over flavours
        for flavour in flavours:
            flavour_name = flavour[0]
            flavour_cpus = flavour[1]
            flavour_memory = flavour[2]

            logger.info('Attempting to deploy on cloud %s with image %s and flavour %s', cloud, image_url, flavour_name)

            # Create complete RADL content
            try:
                radl = Template(str(radl_contents)).substitute(instance=flavour_name,
                                                               image=image_url,
                                                               cloud=cloud,
                                                               ansible_ip=ip_addr,
                                                               ansible_username=username,
                                                               ansible_private_key=private_key)
            except Exception as ex:
                logger.critical('Error creating RADL from template due to %s', ex)
                return False

            # Set total resources used
            cpus_used = flavour_cpus*int(requirements['resources']['instances'])
            memory_used = flavour_memory*int(requirements['resources']['instances'])

            # Deploy infrastructure
            reason = None
            (infra_id, reason) = cloud_deploy.deploy(radl, cloud, time_begin, unique_id, identity, db, int(requirements['resources']['instances']))

            if infra_id:
                success = True
                # Set cloud and IM infra id
                db.deployment_update_status(unique_id, None, cloud, infra_id, resource_type)

                # Final check if we should delete the infrastructure
                (_, infra_status_new, _, _, _) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
                    logger.info('Deletion requested of infrastructure, aborting deployment')
                    return False
                else:
                    # Set status
                    db.deployment_update_status(unique_id, 'configured')
                    db.deployment_update_resources(unique_id, int(requirements['resources']['instances']), cpus_used, memory_used)
                break

    if unique_id and not infra_id:
        logger.info('Setting status to waiting with reason DeploymentFailed')
        db.deployment_update_status(unique_id, 'waiting')
        if reason:
            db.deployment_update_status_reason(unique_id, 'DeploymentFailed_%s' % reason)
        else:
            db.deployment_update_status_reason(unique_id, 'DeploymentFailed')

    return success

