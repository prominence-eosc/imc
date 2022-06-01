from __future__ import print_function
from string import Template
import time
from random import shuffle
import logging

from imc import cloud_deploy
from imc import cloud_utils
from imc import config
from imc import utilities
from imc import cloud_quotas
from imc import policies

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

    # Get requirements & preferences
    requirements = {}
    preferences = {}
    if 'requirements' in description:
        requirements = description['requirements']
    if 'prefereneces' in description:
        preferences = description['preferences']

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
        # If we have already successfully deployed infrastructure we don't need to continue
        if success:
            break

        infra_id = None
        resource_type = None
        region = None
        groups = []
        for cloud_info in clouds_info_list:
            if cloud_info['name'] == cloud:
                resource_type = cloud_info['type']
                region = cloud_info['region']
                if 'supported_groups' in cloud_info:
                    groups = cloud_info['supported_groups']
                break

        if resource_type:
            logger.info('Resource %s is of type %s', cloud, resource_type)
        else:
            logger.info('Skipping because no resource type could be determined for resource %s', cloud)
            continue
        
        # Get image
        try:
            (image_name, image_id) = policy.get_image(cloud)
        except Exception as err:
            logger.critical('Unable to get image due to %s', err)
            return False

        # If no image meets the requirements we should skip the current cloud
        if not image_name:
            logger.info('Skipping because no image could be determined')
            continue

        # Get flavours
        try:
            flavours = policy.get_flavours(cloud)
        except Exception as err:
            logger.critical('Unable to get flavours due to %s', err)
            return False

        # If no flavour meets the requirements we should skip the current cloud
        if not flavours:
            logger.info('Skipping because no flavour could be determined')
            continue

        # Generate list of flavours with unique classs - one class might have no
        # more available hypervisors but another is fine. When maximum resources
        # are specified all flavours will be considered.
        flavours = utilities.create_flavour_list(flavours, requirements)

        logger.info('Attempting to deploy on cloud %s', cloud)
 
        # Check if we should stop
        (_, infra_status_new, _, _, _) = db.deployment_get_im_infra_id(unique_id)
        if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
            logger.info('Deletion requested of infrastructure, aborting deployment')
            return False

        # Loop over flavours
        for flavour in flavours:
            flavour_id = flavour[0]
            flavour_name = flavour[1]
            flavour_cpus = flavour[2]
            flavour_memory = flavour[3]

            logger.info('Attempting to deploy on cloud %s with image %s and flavour %s', cloud, image_name, flavour_name)

            # Update userdata
            userdata_file = CONFIG.get('infrastructure', 'userdata')

            try:
                with open(userdata_file) as fh:
                    userdata = fh.read()
            except Exception as err:
                logger.critical('Error reading userdata template due to: %s', err)
                return False

            try:
                userdata = Template(userdata).substitute(cloud=cloud,
                                                         region=region,
                                                         token=description['token'],
                                                         uid_infra=unique_id)
            except Exception as err:
                logger.critical('Error creating userdata from template due to: %s', err)
                return False

            # Deploy infrastructure
            reason = None
            (infra_id, reason) = cloud_deploy.deploy(userdata,
                                                     image_id,
                                                     flavour_id,
                                                     cloud,
                                                     clouds_info_list,
                                                     time_begin,
                                                     unique_id,
                                                     identity,
                                                     db,
                                                     flavour_cpus,
                                                     flavour_memory)


            if infra_id:
                success = True
                # Set cloud and infra id
                db.deployment_update_status(unique_id, None, cloud, infra_id, resource_type)

                # Final check if we should delete the infrastructure
                (_, infra_status_new, _, _, _) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
                    logger.info('Deletion requested of infrastructure, aborting deployment')
                    return False
                else:
                    # Set status
                    db.deployment_update_status(unique_id, 'configured')
                break # Leave loop over flavours

    if unique_id and not infra_id:
        logger.info('Setting status to waiting with reason DeploymentFailed')
        db.deployment_update_status(unique_id, 'waiting')
        if reason:
            db.deployment_update_status_reason(unique_id, 'DeploymentFailed_%s' % reason)
        else:
            db.deployment_update_status_reason(unique_id, 'DeploymentFailed')

    return success

