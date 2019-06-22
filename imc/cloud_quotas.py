import json
import logging
import sys
import time

from novaclient import client

import opaclient

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

def get_quotas_openstack(cloud, credentials):
    """
    Get quotas remaining for an OpenStack cloud
    """

    auth = {'password':credentials['password'],
            'project_id':credentials['project_id'],
            'project_domain_id':credentials['project_domain_id'],
            'auth_url':credentials['host'],
            'user_domain_name':credentials['user_domain_name']}

    try:
        with client.Client(2, credentials['username'], insecure=True, **auth) as nova:
            quotas = nova.quotas.get(credentials['tenant_id'], detail=True)
    except Exception as ex:
        logger.critical('Unable to get quotas from cloud %s due to "%s"', cloud, ex)
        return (None, None, None)

    quotas_dict = quotas.to_dict()
    cores_available = quotas_dict['cores']['limit'] - quotas_dict['cores']['in_use'] - quotas_dict['cores']['reserved']
    memory_available = quotas_dict['ram']['limit'] - quotas_dict['ram']['in_use'] - quotas_dict['ram']['reserved']
    instances_available = quotas_dict['instances']['limit'] - quotas_dict['instances']['in_use'] - quotas_dict['instances']['reserved']
    return (instances_available, cores_available, int(memory_available/1024))

def set_quotas(requirements, opa_client, config_file):
    """
    Determine the available remaining quotas and set in Open Policy Agent
    """
    try:
        with open(config_file) as file:
            config = json.load(file)
    except Exception as ex:
        logger.critical('Unable to open JSON config file due to: %s', ex)
        return False

    if 'credentials' not in config:
        return False

    for cloud in config['credentials']:
        credentials = config['credentials'][cloud]
        instances = None
        cores = None
        memory = None

        # Check if we need to consider this cloud at all
        if 'sites' in requirements:
            if cloud not in requirements['sites']:
                continue

        if credentials['type'] == 'OpenStack':
            # Check if the cloud hasn't been updated recently
            logger.info('Checking if we need to update cloud %s quotas', cloud)
            update_time = opa_client.get_cloud_update_time(cloud)
            if time.time() - update_time > 60:
                logger.info('Quotas for cloud %s have not been updated recently, so getting current values', cloud)
                (instances, cores, memory) = get_quotas_openstack(cloud, credentials)
        elif credentials['type'] != 'InfrastructureManager':
            logger.warning('Unable to determine quotas for cloud %s of type %s', cloud, credentials['type'])

        if instances is not None and cores is not None and memory is not None:
            logger.info('Setting updated quotas for cloud %s: instances %d, cpus %d, memory %d', cloud, instances, cores, memory)
            opa_client.set_quotas(cloud, instances, cores, memory)
        else:
            logger.info('Not setting updated quotas for cloud %s', cloud)

    return True
