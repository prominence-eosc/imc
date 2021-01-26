"""Functions for checking for & deploying an Ansible node"""
from __future__ import print_function
import re
import sys
from string import Template
import time
from random import shuffle
import logging
import paramiko

from imc import config
from imc import database
from imc import cloud_deploy
from imc import imclient
from imc import tokens
from imc import im_utils
from imc import cloud_utils
from imc import utilities
from imc import policies

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def check_ansible_node(ip_addr, username):
    """
    Check existing Ansible node
    """
    try:
        k = paramiko.RSAKey.from_private_key_file(CONFIG.get('ansible', 'private_key'))
    except Exception as ex:
        logger.crit('Unable to open private key file for Ansible due to: %s', ex)
        return False

    success = False
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip_addr, username=username, pkey=k)
        stdin, stdout, stderr = client.exec_command('pwd')
        client.close()
        success = True
    except Exception as ex:
        logger.info('Unable to execute command on Ansible node with ip %s', ip_addr)
    return success

def delete_ansible_node(cloud, identity, db):
    """
    Delete an Ansible node for the specified cloud
    """
    # Get details about the node
    (infrastructure_id, public_ip, username, timestamp) = db.get_ansible_node(cloud)

    if not infrastructure_id:
        logger.critical('[delete_ansible_node] Unable to get infrastructure id for Ansible node in cloud %s', cloud)
        return False

    logger.info('[delete_ansible_node] About to delete Ansible node from clouds %s with infrastructure id %s', cloud, infrastructure_id)

    # Get full list of cloud info
    clouds_info_list = cloud_utils.create_clouds_list(db, identity)

    #  Get a token if necessary
    token = tokens.get_token(cloud, identity, db, clouds_info_list)

    # Destroy infrastructure
    im_auth = im_utils.create_im_auth(cloud, token, clouds_info_list)
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return False
    (return_code, msg) = client.destroy(infrastructure_id, int(CONFIG.get('timeouts', 'deletion')))
    if return_code != 0:
        logger.critical('Unable to destroy Ansible node infrastructure with id "%s" on cloud "%s" due to "%s"', infrastructure_id, cloud, msg)

    # Delete from the DB
    db.delete_ansible_node(cloud)

    return True

def setup_ansible_node(cloud, identity, db):
    """
    Find or create an Ansible node inside the specified cloud
    """

    # Check if there is a static Ansible node
    (ip_addr, username) = get_static_ansible_node(cloud, db, identity)

    if ip_addr and username:
        logger.info('Found static Ansible node with ip_addr %s on cloud %s', ip_addr, cloud)
        status = check_ansible_node(ip_addr, username)
        if status:
            logger.info('Successfully tested Ansible node with ip %s on cloud %s', ip_addr, cloud)
            return (ip_addr, username)
        else:
            # If we find a static Ansible node and it doesn't work we will not try to create a
            # dynamic one as there must be a reason why a static node was created
            logger.critical('Ansible node with ip %s on cloud %s not accessible', ip_addr, cloud)
            return (None, None)

    logger.info('No functional static Ansible node found for cloud %s', cloud)
    return (None, None)

    # Check if there is a dynamic Ansible node
    (ip_addr, username) = get_dynamic_ansible_node(cloud, db)

    if ip_addr and username:
        logger.info('Found existing dynamic Ansible node with ip %s on cloud %s', ip_addr, cloud)
        status = check_ansible_node(ip_addr, username)
        if status:
            logger.info('Successfully tested Ansible node with ip %s on cloud %s', ip_addr, cloud)
            return (ip_addr, username)
        else:
            logger.info('Ansible node with ip %s on cloud %s not accessible, so deleting', ip_addr, cloud)
            delete_ansible_node(cloud, identity, db)
    logger.info('No functional dynamic Ansible node found for cloud %s', cloud)

    # Try to create a dynamic Ansible node
    infrastructure_id = deploy_ansible_node(cloud, identity, db)

    if not infrastructure_id:
        logger.critical('Unable to create Ansible node on cloud "%s"', cloud)
        return (None, None)

    # Get the public IP
    ip_addr = get_public_ip(infrastructure_id, identity, db)

    if not ip_addr:
        logger.critical('Newly deployed Ansible node has no public IP')
        return (None, None)

    # Set DB
    db.set_ansible_node(cloud, infrastructure_id, ip_addr, 'cloudadm')

    return (ip_addr, 'cloudadm')

def get_dynamic_ansible_node(cloud, db):
    """
    Check if the given cloud has a dynamic Ansible node and return its details if it does
    """
    (infrastructure_id, public_ip, username, timestamp) = db.get_ansible_node(cloud)
    return (public_ip, username)

def get_static_ansible_node(cloud, db, identity):
    """
    Check if the given cloud has a static Ansible node and return it's details if it does
    """
    for cloud_info in cloud_utils.create_clouds_list(db, identity):
        if cloud_info['name'] == cloud:
            if 'ansible' in cloud_info:
                return (cloud_info['ansible']['public_ip'], cloud_info['ansible']['username'])

    return (None, None)

def get_public_ip(infrastructure_id, identity, db):
    """
    Get the public IP of infrastructure
    """
    public_ip = None

    # Get full list of cloud info
    clouds_info_list = cloud_utils.create_clouds_list(db, identity)

    # Setup Infrastructure Manager client
    #FIXME: 1st argument should be cloud
    im_auth = im_utils.create_im_auth(None, None, clouds_info_list)
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return None

    # Get data associated with the Infra ID & find the public IP
    (state, msg) = client.getdata(infrastructure_id, 60)
    m = re.search(r'net_interface.0.ip = \'([\.\d]+)\'', state['data'])
    if m:
        public_ip = m.group(1)

    return public_ip

def deploy_ansible_node(cloud, identity, db):
    """
    Deploy an Ansible node with a public IP address
    """
    logger.info('Deploying Ansible node for cloud "%s"', cloud)

    # Open RADL template file
    try:
        with open(CONFIG.get('ansible', 'template')) as data:
            radl_t = Template(data.read())
    except IOError:
        logger.critical('Unable to open RADL template for Ansible node from file "%s"', CONFIG.get('ansible', 'template'))
        return None

    # Generate requirements for the Ansible node
    requirements = {}
    requirements['resources'] = {}
    requirements['resources']['cores'] = int(CONFIG.get('ansible', 'cores'))
    requirements['resources']['memory'] = int(CONFIG.get('ansible', 'memory'))

    requirements['image'] = {}
    requirements['image']['architecture'] = CONFIG.get('ansible', 'architecture')
    requirements['image']['distribution'] = CONFIG.get('ansible', 'distribution')
    requirements['image']['type'] = CONFIG.get('ansible', 'type')
    requirements['image']['version'] = CONFIG.get('ansible', 'version')

    # Generate JSON to be given to the policy engine
    userdata = {'requirements':requirements, 'preferences':{}}

    # Get full list of cloud info
    logger.info('Getting list of clouds from DB')
    clouds_info_list = cloud_utils.create_clouds_list(db, identity)

    # Setup policy engine
    logger.info('Setting up policies')
    policy = policies.PolicyEngine(clouds_info_list, userdata, db, identity)

    # Get the image & flavour
    try:
        (_, image) = policy.get_image(cloud)
    except Exception as err:
        logger.critical('Unable to get image due to %s', err)
        return False

    try:
        flavour = policy.get_flavour(cloud)
    except Exception as err:
        logger.critical('Unable to get flavour due to %s', err)
        return False

    if not image:
        logger.critical('Unable to deploy Ansible node because no acceptable image is available')
        return None

    if not flavour:
        logger.critical('Unable to deploy Ansible node because no acceptable flavour is available')
        return None

    logger.info('Using image "%s" and flavour "%s" to deploy Ansible node', image, flavour)

    # Get the private & public keys
    try:
        with open(CONFIG.get('ansible', 'private_key')) as data:
            private_key = data.read()
    except IOError:
        logger.critical('Unable to open private key for Ansible node from file "%s"', filename)
        return None

    try:
        with open(CONFIG.get('ansible', 'public_key')) as data:
            public_key = data.read()
    except IOError:
        logger.critical('Unable to open public key for Ansible node from file "%s"', filename)
        return None

    # Create complete RADL content
    try:
        radl = radl_t.substitute(instance=flavour,
                                 image=image,
                                 private_key=private_key,
                                 public_key=public_key,
                                 cloud=cloud)
    except Exception as e:
        logger.critical('Error creating RADL from template for Ansible node due to %s', e)
        return None

    time_begin = time.time()

    # Deploy infrastructure
    infra_id = cloud_deploy.deploy(radl, cloud, time_begin, None, db)

    return infra_id

