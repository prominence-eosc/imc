import logging

from imc import config
from imc import policies
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def return_sites(description):
    """
    Find appropriate resources to deploy infrastructure, but don't do anything
    """
    # Get RADL
    radl_contents = utilities.get_radl(description)
    if not radl_contents:
        logging.critical('RADL must be provided')
        return None

    # Get requirements & preferences
    (requirements, preferences) = utilities.get_reqs_and_prefs(description)

    # Count number of instances
    instances = utilities.get_num_instances(radl_contents)
    logger.info('Found %d instances to deploy', instances)
    requirements['resources']['instances'] = instances

    # Setup policy engine
    logger.info('Setting up policies')
    policy = policies.PolicyEngine(clouds_info_list,
                                   {'requirements':requirements,
                                    'preferences':preferences,
                                    'ignore_usage': True},
                                   db,
                                   identity)

    # Check if deployment could be possible, ignoring current quotas/usage
    logger.info('Checking if job requirements will match any clouds')
    clouds = policy.statisfies_requirements(ignore_usage=True)

    logger.info('Suitable resources = [%s]', ','.join(clouds))

    return clouds
