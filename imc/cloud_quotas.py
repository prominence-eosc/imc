"""Get cloud quotas & usage"""
#TODO: set static quotas (i.e. limits) as well from here

import logging
import time

from novaclient import client

from imc import config
from imc import tokens
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def get_quotas_openstack(cloud, credentials, token):
    """
    Get quotas remaining for an OpenStack cloud
    """
    if 'password' not in credentials:
        logger.critical('password is not in the credentials file')
        return {}
    if 'project_id' not in credentials:
        logger.critical('project_id is not in the credentials file')
        return {}
    if 'project_domain_id' not in credentials:
        logger.critical('project_domain_id is not in the credentials file')
        return {}
    if 'host' not in credentials:
        logger.critical('host is not in the credentials file')
        return {}
    if 'user_domain_name' not in credentials:
        logger.critical('user_domain_name is not in the credentials file')
        return {}

    auth = {'project_id':credentials['project_id'],
            'project_domain_id':credentials['project_domain_id'],
            'auth_url':credentials['host'],
            'user_domain_name':credentials['user_domain_name']}

    if token:
        auth['auth_token'] = token
    elif 'password' in credentials:
        auth['password'] = credentials['password']
    else:
        logger.critical('Do not have a token for cloud %s', cloud)
        return {}

    quotas = {}

    # Get limits only first, as some clouds don't allow users to get their own usage info (why??)
    try:
        nova = client.Client(2, credentials['username'], timeout=10, **auth)
        os_quotas = nova.quotas.get(credentials['tenant_id'], detail=False)
    except Exception as ex:
        logger.warning('Unable to get quotas from cloud %s due to "%s"', cloud, str(ex).encode('utf-8'))
        return quotas

    os_quotas_dict = os_quotas.to_dict()

    quotas['cpu-limit'] = os_quotas_dict['cores']
    quotas['memory-limit'] = int(os_quotas_dict['ram']/1024)
    quotas['instances-limit'] = os_quotas_dict['instances']

    logger.info('Got limits cpu=%d, memory=%d, instances=%d', int(quotas['cpu-limit']), quotas['memory-limit'], int(quotas['instances-limit']))

    # Try to get usage now
    try:
        nova = client.Client(2, credentials['username'], timeout=10, **auth)
        os_quotas = nova.quotas.get(credentials['tenant_id'], detail=True)
    except Exception as ex:
        logger.critical('Unable to get quota usage from cloud %s due to "%s"', cloud, str(ex).encode('utf-8'))
        return quotas

    os_quotas_dict = os_quotas.to_dict()

    quotas['cpu-used'] = os_quotas_dict['cores']['in_use'] + os_quotas_dict['cores']['reserved']
    quotas['memory-used'] = int(os_quotas_dict['ram']['in_use'] + os_quotas_dict['ram']['reserved'])/1024
    quotas['instances-used'] = os_quotas_dict['instances']['in_use'] + os_quotas_dict['instances']['reserved']

    logger.info('Got usage cpu=%d, memory=%d, instances=%d', int(quotas['cpu-used']), quotas['memory-used'], int(quotas['instances-used']))

    return quotas

def set_quotas(requirements, db, identity, config):
    """
    Determine the available remaining quotas
    """
    for cloud in config:
        name = cloud['name']
        logger.info('[set_quotas] Considering cloud %s', name)
        credentials = cloud['credentials']

        instances = None
        cores = None
        memory = None

        instances_static = -1
        cores_static = -1
        memory_static = -1

        # Check if we need to consider this cloud at all
        if 'sites' in requirements:
            if name not in requirements['sites'] and requirements['sites']:
                continue

        if cloud['type'] != 'cloud':
            continue

        # Check for hardwired quotas in config file
        if 'quotas' in cloud:
            if 'instances' in cloud['quotas']:
                instances_static = cloud['quotas']['instances']
            if 'cores' in cloud['quotas']:
                cores_static = cloud['quotas']['cores']
            if 'memory' in cloud['quotas']:
                memory_static = cloud['quotas']['memory']

        # Get a token if necessary
        token = tokens.get_token(name, identity, db, config)

        if credentials['type'] == 'OpenStack':
            # Get a scoped token if necessary from Keystone
            if token:
                logger.info('Getting a scoped token from Keystone')
                try:
                    token = tokens.get_scoped_token(credentials['host'], credentials['project_id'],
                                                    tokens.get_unscoped_token(credentials['host'],
                                                                              token,
                                                                              credentials['username'],
                                                                              credentials['tenant']))
                except:
                    logger.critical('Unable to get a scoped token from Keystone due to a timeout')
                    continue

            # Check if the cloud hasn't been updated recently
            logger.info('Checking if we need to update cloud %s quotas', name)
            use_identity = identity
            if cloud['source'] == 'static':
                use_identity = 'static'

            last_update = db.get_cloud_updated_quotas(name, use_identity)

            if time.time() - last_update <= int(CONFIG.get('updates', 'quotas')):
                logger.info('Quotas updatede too recetnly, will not update')
 
            if time.time() - last_update > int(CONFIG.get('updates', 'quotas')):
                logger.info('Quotas for cloud %s have not been updated recently, so getting current values', name)
                quotas = get_quotas_openstack(name, credentials, token)

                if 'cpu-limit' in quotas:
                    logger.info('Setting static quotas in DB for cloud %s', name)

                    cores_limit = quotas['cpu-limit']
                    memory_limit = quotas['memory-limit']
                    instances_limit = quotas['instances-limit']

                    if cores_static < cores_limit:
                        cores_limit = cores_static

                    if memory_static < memory_limit:
                        memory_limit = memory_static

                    if instances_static < instances_limit:
                        instances_limit = instances_static

                    db.set_cloud_static_quotas(name, use_identity, cores_limit, memory_limit, instances_limit)
                    db.set_cloud_updated_quotas(name, use_identity)

                # For clouds which do not allow users to get the used resources of their own project (i.e. many
                # OpenStack clouds in EGI FedCloud) we use our own estimate of the used resources. For other
                # clouds we add our own estimate of the resource usage of infrastructure currently being
                # provisioned to the cloud-provided usage
                # TODO: don't store this in DB, but add to what's in DB?
                if 'cpu-used' not in quotas:
                    logger.info('Unable to get used resources from API, will use our own estimate instead')
                    (used_instances, used_cpus, used_memory) = db.get_used_resources(use_identity, name, True)
                    quotas['instances-used'] = used_instances
                    quotas['cpu-used'] = used_cpus
                    quotas['memory-used'] = used_memory
                else:
                    logger.info('Including our own usage of resources currently being deployed')
                    (used_instances, used_cpus, used_memory) = db.get_used_resources(use_identity, name)
                    quotas['instances-used'] += used_instances
                    quotas['cpu-used'] += used_cpus
                    quotas['memory-used'] += used_memory

                instances = quotas['instances-limit'] - quotas['instances-used']
                cores = quotas['cpu-limit'] - quotas['cpu-used']
                memory = quotas['memory-limit'] - quotas['memory-used']

                if instances and cores and memory:
                    logger.info('Got our own, resources valid')
                else:
                    logger.info('Got our own, resources not vslid')
                    
        elif credentials['type'] != 'InfrastructureManager':
            logger.warning('Unable to determine quotas for cloud %s of type %s', name, credentials['type'])

        if instances and cores and memory:
            logger.info('Setting updated quotas for cloud %s: instances %d, cpus %d, memory %d', name, instances, cores, memory)
            db.set_cloud_dynamic_quotas(name, use_identity, cores, memory, instances)
        else:
            logger.info('Not setting updated quotas for cloud %s', name)

    return True
