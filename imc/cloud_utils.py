"""Miscellaneous cloud functions"""
from __future__ import print_function
import glob
import json
import logging

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

from imc import config

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def create_clouds_list_egi(db, identity):
    """
    Create list of EGI FedCloud sites from the DB
    """
    clouds = []
    clouds_from_db = db.get_egi_clouds(identity)
    for site in clouds_from_db:
        cloud = clouds_from_db[site]
        cloud['credentials']['username'] = 'egi.eu'
        cloud['credentials']['password'] = 'token'
        cloud['credentials']['auth_version'] = '3.x_oidc_access_token'
        cloud['credentials']['token'] = {}
        cloud['credentials']['token']['provider'] = 'user'
        cloud['credentials']['token']['client_id'] = CONFIG.get('egi.credentials', 'client_id')
        cloud['credentials']['token']['client_secret'] = CONFIG.get('egi.credentials', 'client_secret')
        cloud['credentials']['token']['scope'] = CONFIG.get('egi.credentials', 'scope')
        cloud['credentials']['token']['url'] = CONFIG.get('egi.credentials', 'url')
        cloud['type'] = 'cloud'
        cloud['enabled'] = True
        cloud['region'] = CONFIG.get('egi', 'region')
        cloud['tags'] = {}
        cloud['tags']['multi-node-jobs'] = 'false'
        cloud['ansible'] = {}
        cloud['quotas'] = {}
        cloud['supported_groups'] = []
        cloud['image_prefix'] = ''
        cloud['image_templates'] = {}
        cloud['default_flavours'] = {}
        cloud['flavour_filters'] = {}
        cloud['default_images'] = {}
        name = CONFIG.get('egi.image', 'name').replace('site', site)
        cloud['default_images'][name] = {}
        cloud['default_images'][name]['name'] = name
        cloud['default_images'][name]['architecture'] = CONFIG.get('egi.image', 'architecture')
        cloud['default_images'][name]['distribution'] = CONFIG.get('egi.image', 'distribution')
        cloud['default_images'][name]['type'] = CONFIG.get('egi.image', 'type')
        cloud['default_images'][name]['version'] = CONFIG.get('egi.image', 'version')
        cloud['images'] = cloud['default_images']

        clouds.append(cloud)

    return clouds

def connect_to_cloud(cloud, config, token):
    """
    Connect to a cloud using LibCloud
    """
    if config['credentials']['type'] == 'OpenStack':
        details = {}
        if config['credentials']['auth_version'] == '3.x_password':
            details['ex_force_auth_url'] = config['credentials']['host']
            if 'auth_version' in config['credentials']:
                details['ex_force_auth_version'] = config['credentials']['auth_version']
            if 'tenant' in config['credentials']:
                details['ex_tenant_name'] = config['credentials']['tenant']
            if 'domain' in config['credentials']:
                details['ex_domain_name'] = config['credentials']['domain']
            if 'service_region' in config['credentials']:
                details['ex_force_service_region'] = config['credentials']['service_region']
            if 'project_domain_id' in config['credentials']:
                details['ex_tenant_domain_id'] = config['credentials']['project_domain_id']

            provider = get_driver(Provider.OPENSTACK)
            try:
                conn = provider(config['credentials']['username'],
                                config['credentials']['password'],
                                **details)
            except Exception as ex:
                logger.critical('Unable to connect to cloud %s due to "%s"', cloud, ex)
                return None
        elif config['credentials']['auth_version'] == '3.x_oidc_access_token':
            details['ex_force_auth_url'] = config['credentials']['host']
            if 'auth_version' in config['credentials']:
                details['ex_force_auth_version'] = config['credentials']['auth_version']
            if 'tenant' in config['credentials']:
                details['ex_tenant_name'] = config['credentials']['tenant']
            if 'domain' in config['credentials']:
                details['ex_domain_name'] = config['credentials']['domain']
            if 'service_region' in config['credentials']:
                details['ex_force_service_region'] = config['credentials']['service_region']

            provider = get_driver(Provider.OPENSTACK)
            try:
                conn = provider(config['credentials']['username'],
                                token,
                                **details)
            except Exception as ex:
                logger.critical('Unable to connect to cloud %s due to "%s"', cloud, ex)
                return None
        else:
            return None

    elif config['credentials']['type'] == 'GCE':
        details = {}
        if 'project' in config['credentials']:
            details['project'] = config['credentials']['project']
        if 'datacenter' in config['credentials']:
            details['datacenter'] = config['credentials']['datacenter']

        provider = get_driver(Provider.GCE)
        try:
            conn = provider(config['credentials']['username'],
                            config['credentials']['password'],
                            **details)
        except Exception as ex:
            logger.critical('Unable to connect to cloud %s due to "%s"', cloud, ex)
            return None

    else:
        return None

    return conn

def create_clouds_list_static(path):
    """
    Generate list of static clouds
    """
    clouds = []

    cloud_files = glob.glob('%s/*.json' % path)
    for cloud_file in cloud_files:
        data = {}
        try:
            with open(cloud_file) as fd:
                data = json.load(fd)
        except Exception as err:
            logger.error('Unable to read static cloud file %s due to: %s', cloud_file, err)

        if data:
            clouds.append(data)

    return clouds

def create_clouds_list(db, identity):
    """
    Generate full list of clouds
    """
    if CONFIG.get('egi', 'enabled').lower() == 'true':
        logger.info('Getting list of clouds from EGI')
        list_egi = create_clouds_list_egi(db, identity)
    else:
        list_egi = []

    logger.info('Getting list of clouds from static JSON files')
    list_static = create_clouds_list_static(CONFIG.get('clouds', 'path'))

    full_list = list_egi + list_static

    for site in full_list:
        if site['name'] in CONFIG.get('egi', 'blacklist').split(','):
            full_list.remove(site)

    return full_list
