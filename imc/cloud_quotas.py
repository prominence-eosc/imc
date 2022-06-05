"""Get cloud quotas & usage"""
import logging
import time

from imc import config
from imc import tokens
from imc import utilities
from imc import resources

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def set_quotas(requirements, db, identity, config):
    """
    Determine the available remaining quotas
    """
    for cloud in config:
        name = cloud['name']
        logger.info('Considering cloud %s', name)
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
        cloud = tokens.get_openstack_token(token, cloud)

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
            client = resources.Resource(cloud)
            quotas = client.get_quotas()

            if not quotas:
                logger.info('Did not any quotas from cloud %s', name)
                continue

            if 'limits' not in quotas:
                logger.info('Did not obtain limits from cloud %s', name)
                continue

            logger.info('Setting static quotas in DB for cloud %s', name)

            cores_limit = quotas['limits']['cpus']
            memory_limit = quotas['limits']['memory']
            instances_limit = quotas['limits']['instances']

            if cores_static < cores_limit and cores_static > -1:
                cores_limit = cores_static

            if memory_static < memory_limit and memory_static > -1:
                memory_limit = memory_static

            if instances_static < instances_limit and instances_static > -1:
                instances_limit = instances_static

            db.set_cloud_static_quotas(name, use_identity, cores_limit, memory_limit, instances_limit)
            db.set_cloud_updated_quotas(name, use_identity)

            # For clouds which do not allow users to get the used resources of their own project (i.e. many
            # OpenStack clouds in EGI FedCloud) we use our own estimate of the used resources. For other
            # clouds we add our own estimate of the resource usage of infrastructure currently being
            # provisioned to the cloud-provided usage
            # TODO: don't store this in DB, but add to what's in DB?
            if 'usage' not in quotas:
                logger.info('Unable to get used resources from API, will use our own estimate instead')
                (used_instances, used_cpus, used_memory) = db.get_used_resources(use_identity, name, True)
                quotas['usage']['instances'] = used_instances
                quotas['usage']['cpus'] = used_cpus
                quotas['usage']['memory'] = used_memory
            else:
                logger.info('Including our own usage of resources currently being deployed')
                (used_instances, used_cpus, used_memory) = db.get_used_resources(use_identity, name)
                quotas['usage']['instances'] += used_instances
                quotas['usage']['cpus'] += used_cpus
                quotas['usage']['memory'] += used_memory

            instances = quotas['limits']['instances'] - quotas['usage']['instances']
            cores = quotas['limits']['cpus'] - quotas['usage']['cpus']
            memory = quotas['limits']['memory'] - quotas['usage']['memory']

            if instances and cores and memory:
                logger.info('Got our own, resources valid')
            else:
                logger.info('Got our own, resources not vslid')
                    
        if instances and cores and memory:
            logger.info('Setting updated quotas for cloud %s: instances %d, cpus %d, memory %d', name, instances, cores, memory)
            db.set_cloud_dynamic_quotas(name, use_identity, cores, memory, instances)
        else:
            logger.info('Not setting updated quotas for cloud %s', name)

    return True
