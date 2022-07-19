"""Miscellaneous cloud functions"""
import glob
import json
import logging

from imc import config
from imc import database

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
        cloud['token_source'] = {}
        cloud['token_source']['client_id'] = CONFIG.get('egi.credentials', 'client_id')
        cloud['token_source']['client_secret'] = CONFIG.get('egi.credentials', 'client_secret')
        cloud['token_source']['scope'] = CONFIG.get('egi.credentials', 'scope')
        cloud['token_source']['url'] = CONFIG.get('egi.credentials', 'url')
        cloud['type'] = 'cloud'
        cloud['enabled'] = True
        cloud['source'] = 'egi'
        cloud['networks'] = []
        cloud['resource_type'] = 'OpenStack'
        cloud['region'] = CONFIG.get('egi', 'region')
        cloud['tags'] = {}
        cloud['tags']['multi-node-jobs'] = 'false'
        cloud['quotas'] = {}
        cloud['supported_groups'] = []
        cloud['image_templates'] = {}
        cloud['image_templates'][CONFIG.get('egi.image', 'name')] = {}
        cloud['image_templates'][CONFIG.get('egi.image', 'name')]['architecture'] = CONFIG.get('egi.image', 'architecture')
        cloud['image_templates'][CONFIG.get('egi.image', 'name')]['distribution'] = CONFIG.get('egi.image', 'distribution')
        cloud['image_templates'][CONFIG.get('egi.image', 'name')]['type'] = CONFIG.get('egi.image', 'type')
        cloud['image_templates'][CONFIG.get('egi.image', 'name')]['version'] = CONFIG.get('egi.image', 'version')
        cloud['default_flavours'] = []
        cloud['flavour_filters'] = {}
        cloud['default_images'] = []

        clouds.append(cloud)

    return clouds

def create_clouds_list_static(db, identity):
    """
    Generate list of static clouds
    """
    clouds = db.list_resources(identity)
    return clouds

def create_clouds_list(db, identity, static=True):
    """
    Generate full list of clouds
    """
    if CONFIG.get('egi', 'enabled').lower() == 'true':
        logger.info('Getting list of clouds from EGI')
        list_egi = create_clouds_list_egi(db, identity)
    else:
        list_egi = []

    if static:
        logger.info('Getting list of clouds from static JSON files')
        list_static = create_clouds_list_static(db, identity)
    else:
        list_static = []

    full_list = list_egi + list_static

    for site in full_list:
        if site['name'] in CONFIG.get('egi', 'blacklist').split(','):
            full_list.remove(site)

    return full_list

def check_for_new_clouds(db, identity):
    """
    Check if any new clouds have been defined
    """
    clouds_list = create_clouds_list(db, identity)
    new_clouds = False

    for cloud in clouds_list:
        name = cloud['name']
        (status, _, _, _, _, _, _, _) = db.get_cloud_info(name, identity)
        if status is None:
            new_clouds = True

    return new_clouds
