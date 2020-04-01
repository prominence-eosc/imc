import logging

from imc import config
from imc import opaclient
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

    # Generate JSON to be given to Open Policy Agent
    userdata = {'requirements':requirements, 'preferences':preferences, 'ignore_usage': True}

    # Setup Open Policy Agent client
    opa_client = opaclient.OPAClient(url=CONFIG.get('opa', 'url'), timeout=int(CONFIG.get('opa', 'timeout')))

    # Check if deployment could be possible, ignoring current quotas/usage
    logger.info('Checking if job requirements will match any clouds')
    try:
        clouds = opa_client.get_clouds(userdata)
    except Exception as err:
        logger.critical('Unable to get list of clouds due to %s:', err)
        return None

    logger.info('Suitable resources = [%s]', ','.join(clouds))

    return clouds
