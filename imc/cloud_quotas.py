import json
import logging
import os
import sys
import time
import ConfigParser

from novaclient import client

import opaclient
import tokens

# Configuration
CONFIG = ConfigParser.ConfigParser()
if 'PROMINENCE_IMC_CONFIG_DIR' in os.environ:
    CONFIG.read('%s/imc.ini' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
else:
    print('ERROR: Environment variable PROMINENCE_IMC_CONFIG_DIR has not been defined')
    exit(1)


# Logging
logger = logging.getLogger(__name__)

def get_quotas_openstack(cloud, credentials, token):
    """
    Get quotas remaining for an OpenStack cloud
    """
    if 'password' not in credentials:
        logger.critical('password is not in the credentials file')
        return (None, None, None)
    if 'project_id' not in credentials:
        logger.critical('project_id is not in the credentials file')
        return (None, None, None)
    if 'project_domain_id' not in credentials:
        logger.critical('project_domain_id is not in the credentials file')
        return (None, None, None)
    if 'host' not in credentials:
        logger.critical('host is not in the credentials file')
        return (None, None, None)
    if 'user_domain_name' not in credentials:
        logger.critical('user_domain_name is not in the credentials file')
        return (None, None, None)

    auth = {'project_id':credentials['project_id'],
            'project_domain_id':credentials['project_domain_id'],
            'auth_url':credentials['host'],
            'user_domain_name':credentials['user_domain_name']}

    if token:
        auth['auth_token'] = token
    else:
        auth['password'] = credentials['password']

    try:
        nova = client.Client(2, credentials['username'], timeout=30, **auth)
        quotas = nova.quotas.get(credentials['tenant_id'], detail=True)
    except Exception as ex:
        logger.critical('Unable to get quotas from cloud %s due to "%s"', cloud, ex)
        return (None, None, None)

    quotas_dict = quotas.to_dict()
    cores_available = quotas_dict['cores']['limit'] - quotas_dict['cores']['in_use'] - quotas_dict['cores']['reserved']
    memory_available = quotas_dict['ram']['limit'] - quotas_dict['ram']['in_use'] - quotas_dict['ram']['reserved']
    instances_available = quotas_dict['instances']['limit'] - quotas_dict['instances']['in_use'] - quotas_dict['instances']['reserved']
    return (instances_available, cores_available, int(memory_available/1024))

def set_quotas(requirements, db, identity, opa_client, config):
    """
    Determine the available remaining quotas and set in Open Policy Agent
    """
    for cloud in config:
        name = cloud['name']
        credentials = cloud['credentials']
        instances = None
        cores = None
        memory = None

        # Check if we need to consider this cloud at all
        if 'sites' in requirements:
            if name not in requirements['sites']:
                continue

        # Get a token if necessary
        token = tokens.get_token(name, identity, db, config)

        if credentials['type'] == 'OpenStack':
            # Get a scoped token if necessary from Keystone
            if token:
                logger.info('Getting a scoped token from Keystone')
                token = tokens.get_scoped_token(credentials['host'], credentials['project_id'],
                                                tokens.get_unscoped_token(credentials['host'],
                                                                          token,
                                                                          credentials['username'],
                                                                          credentials['tenant']))

            # Check if the cloud hasn't been updated recently
            logger.info('Checking if we need to update cloud %s quotas', name)
            try:
                update_time = opa_client.get_quota_update_time(name)
            except Exception as err:
                logger.critical('Unable to get quota update time due to:', err)
                return False
 
            if time.time() - update_time > int(CONFIG.get('updates', 'quotas')):
                logger.info('Quotas for cloud %s have not been updated recently, so getting current values', name)
                (instances, cores, memory) = get_quotas_openstack(name, credentials, token)
        elif credentials['type'] != 'InfrastructureManager':
            logger.warning('Unable to determine quotas for cloud %s of type %s', name, credentials['type'])

        if instances and cores and memory:
            logger.info('Setting updated quotas for cloud %s: instances %d, cpus %d, memory %d', name, instances, cores, memory)
            opa_client.set_quotas(name, instances, cores, memory)
        else:
            logger.info('Not setting updated quotas for cloud %s', name)

    return True
