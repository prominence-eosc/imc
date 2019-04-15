#!/usr/bin/python

from __future__ import print_function
from concurrent.futures import ProcessPoolExecutor
import base64
import os
import sys
import re
from string import Template
import tempfile
import json
import time
from random import shuffle
import logging
import uuid
import ConfigParser
import sqlite3
import requests
from flask import Flask, request, jsonify
import paramiko
import imclient

app = Flask(__name__)

def db_init():
    """
    Initialize database
    """

    # Setup database table if necessary
    try:
        db = sqlite3.connect(CONFIG.get('ansible', 'db'))
        cursor = db.cursor()

        # Create Ansible nodes table
        cursor.execute('''CREATE TABLE IF NOT EXISTS
                          ansible_nodes(cloud TEXT NOT NULL PRIMARY KEY,
                                        infrastructure_id TEXT NOT NULL,
                                        public_ip TEXT NOT NULL,
                                        username TEXT NOT NULL,
                                        creation DATETIME DEFAULT CURRENT_TIMESTAMP,
                                        last_used DATETIME DEFAULT CURRENT_TIMESTAMP
                                        )''')

        # Create credentials table
        cursor.execute('''CREATE TABLE IF NOT EXISTS
                          credentials(cloud TEXT NOT NULL PRIMARY KEY,
                                      token TEXT NOT NULL,
                                      expiry INT NOT NULL,
                                      creation INT NOT NULL
                                      )''')

        # Create deployments table
        cursor.execute('''CREATE TABLE IF NOT EXISTS
                          deployments(id TEXT NOT NULL PRIMARY KEY,
                                      status TEXT NOT NULL,
                                      im_infra_id TEXT,
                                      cloud TEXT,
                                      creation INT NOT NULL
                                      )''')


        db.commit()
        db.close()
    except Exception as ex:
        print(ex)
        exit(1)

# Configuration
CONFIG = ConfigParser.ConfigParser()
if 'PROMINENCE_IMC_CONFIG_DIR' in os.environ:
    CONFIG.read('%s/imc.ini' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
else:
    print('ERROR: Environment variable PROMINENCE_IMC_CONFIG_DIR has not been defined')
    exit(1)

# Setup process pool for handling deployments
executor = ProcessPoolExecutor(int(CONFIG.get('pool','size')))

# Logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger('imc')

 # Interacting with Open Policy Agent
OPA_TIMEOUT = int(CONFIG.get('opa', 'timeout'))
OPA_URL = CONFIG.get('opa', 'url')

# Initialize DB
db_init()

@app.route('/infrastructures', methods=['POST'])
def create_infrastructure():
    """
    Create infrastructure
    """
    uid = str(uuid.uuid4())
    executor.submit(imc_deploy, request.get_json(), uid)
    return jsonify({'id':uid}), 201

@app.route('/infrastructures/<string:infra_id>', methods=['GET'])
def get_infrastructure(infra_id):
    """
    Get current status of specified infrastructure
    """
    (im_infra_id, status, cloud) = imc_status(infra_id)
    if status is not None:
        return jsonify({'status':status, 'cloud':cloud, 'infra_id':im_infra_id}), 200
    return jsonify({'status':'invalid'}), 400

@app.route('/infrastructures/<string:infra_id>', methods=['DELETE'])
def delete_infrastructure(infra_id):
    """
    Delete the specified infrastructure
    """
    executor.submit(imc_delete, infra_id)
    return jsonify({}), 200

def get_token(cloud):
    """
    Get a token for a cloud
    """
    logger.info('Checking if we need a token for cloud %s', cloud)

    # Get details required for generating a new token
    (username, password, client_id, client_secret, refresh_token, scope, url) = check_if_token_required(cloud)
    if username is None or password is None or client_id is None or client_secret is None or refresh_token is None or scope is None or url is None:
        logger.info('A token is not required for cloud %s', cloud)
        return None

    # Try to obtain an existing token from the DB
    logger.info('Try to get an existing token from the DB')
    (token, expiry, creation) = db_get_token(cloud)

    # Check token
    if token is not None:
        check_rt = check_token(token, url)
        if check_rt != 0:
            logger.info('Check token failed for cloud %s', cloud)
    else:
        logger.info('No token could be obtained from the DB for cloud %s', cloud)
        check_rt = -1

    logger.info('Token expiry time: %d, current time: %d', expiry, time.time())
    if expiry - time.time() < 600:
        logger.info('Token has or is about to expire')

    if token is None or expiry - time.time() < 600 or (check_rt != 0 and time.time() - creation > 600):
        logger.info('Getting a new token for cloud %s', cloud)
        # Get new token
        (token, expiry, creation, msg) = get_new_token(username, password, client_id, client_secret, refresh_token, scope, url)

        # Delete existing token from DB
        db_delete_token(cloud)

        # Save token to DB
        db_set_token(cloud, token, expiry, creation)
    else:
        logger.info('Using token from DB for cloud %s', cloud)

    return token

def create_im_auth(cloud, token):
    """
    Create the "auth file" required for requests to IM, inserting tokens as necessary
    """
    try:
        with open('%s/imc.json' % os.environ['PROMINENCE_IMC_CONFIG_DIR']) as file:
            data = json.load(file)
    except Exception as ex:
        logger.critical('Unable to load JSON config file due to: %s', ex)
        return None

    info1 = {}
    info2 = {}
    if token is not None:
        info1['token'] = token
    else:
        info2['token'] = 'not-required'

    im_auth_file = ''
    for item in data['im']['auth']:
        line = '%s\\n' % data['im']['auth'][item]
        if item == cloud and token is not None:
            line = line % info1
        else:
            line = line % info2
        line = line.replace('\n', '____n')
        im_auth_file += line

    return im_auth_file

def get_new_token(username, password, client_id, client_secret, refresh_token, scope, url):
    """
    Get a new access token using a refresh token
    """
    creation = time.time()
    data = {'client_id':client_id,
            'client_secret':client_secret,
            'grant_type':'refresh_token',
            'refresh_token':refresh_token,
            'scope':scope}
    try:
        response = requests.post(url + '/token', auth=(username, password), timeout=10, data=data)
    except requests.exceptions.Timeout:
        return (None, 0, 0, 'timed out')
    except requests.exceptions.RequestException as ex:
        return (None, 0, 0, ex)

    if response.status_code == 200:
        access_token = response.json()['access_token']
        expires_at = int(response.json()['expires_in'] + creation)
        return (access_token, expires_at, creation, '')
    return (None, 0, 0, response.text)

def check_token(token, url):
    """
    Check whether a token is valid
    """
    header = {"Authorization":"Bearer %s" % token}

    try:
        response = requests.get(url + '/userinfo', headers=header, timeout=10)
    except requests.exceptions.Timeout:
        return 2
    except requests.exceptions.RequestException:
        return 2

    if response.status_code == 200:
        return 0
    return 1

def check_if_token_required(cloud):
    """
    Check if the given cloud requires a token for access
    """
    try:
        with open('/etc/prominence/imc.json') as file:
            data = json.load(file)
    except Exception as ex:
        logger.critical('Unable to open file containing tokens due to: %s', ex)
        return (None, None, None, None, None, None, None)

    if 'credentials' in data:
        if cloud in data['credentials']:
            return (data['credentials'][cloud]['username'],
                    data['credentials'][cloud]['password'],
                    data['credentials'][cloud]['client_id'],
                    data['credentials'][cloud]['client_secret'],
                    data['credentials'][cloud]['refresh_token'],
                    data['credentials'][cloud]['scope'],
                    data['credentials'][cloud]['url'])

    return (None, None, None, None, None, None, None)

def destroy(client, infrastructure_id, cloud):
    """
    Destroy the specified infrastructure, including retries since clouds can be unreliable
    """
    count = 0
    delay_factor = float(CONFIG.get('deletion', 'factor'))
    delay = delay_factor
    destroyed = False
    while not destroyed and count < int(CONFIG.get('deletion', 'retries')):
        (return_code, msg) = client.destroy(infrastructure_id, int(CONFIG.get('timeouts', 'deletion')))
        if return_code == 0:
            destroyed = True
        else:
            logger.warning('Unable to destroy infrastructure with id "%s" on cloud "%s", attempt %d, due to "%s"', infrastructure_id, cloud, count, msg)
        count += 1
        delay = delay*delay_factor
        time.sleep(int(count + delay))
    if not destroyed:
        logger.critical('Unable to destroy infrastructure with id "%s" on cloud "%s" due to "%s"', infrastructure_id, cloud, msg)
    return destroyed

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

def db_connect():
    """
    Connect to the DB
    """
    try:
        db = sqlite3.connect(CONFIG.get('ansible', 'db'))
    except Exception as ex:
        logger.critical('[db_connect] Unable to connect to sqlite DB because of %s', ex)
        return None
    return db

def db_close(db):
    db.close

def db_deployment_get_im_infra_id(db, infra_id):
    im_infra_id = None
    status = None
    cloud = None

    try:
        cursor = db.cursor()
        cursor.execute('SELECT im_infra_id,status,cloud FROM deployments WHERE id="%s"' % infra_id)

        for row in cursor:
            im_infra_id = row[0]
            status = row[1]
            cloud = row[2]

    except Exception as e:
        logger.critical('[db_get] Unable to connect to sqlite DB because of %s', e)

    return (im_infra_id, status, cloud)

def db_deployment_create_with_retries(db, infra_id):
    """
    Create deployment with retries & backoff
    """
    max_retries = 10
    count = 0
    success = False
    while count < max_retries and not success:
        success = db_deployment_create(db, infra_id)
        if not success:
            count += 1
            db_close(db)
            time.sleep(count/2)
            db = db_connect()
    return success

def db_deployment_create(db, infra_id):
    """
    Create deployment
    """
    try:
        cursor = db.cursor()
        cursor.execute('INSERT INTO deployments (id,status,creation) VALUES ("%s","accepted",%d)' % (infra_id, time.time()))
        db.commit()
    except Exception as e:
        logger.critical('[db_set] Unable to connect to sqlite DB because of %s', e)
        return False
    return True

def db_deployment_update_infra_with_retries(db, infra_id):
    """
    Create deployment with retries & backoff
    """
    max_retries = 10
    count = 0
    success = False
    while count < max_retries and not success:
        success = db_deployment_update_infra(db, infra_id)
        if not success:
            count += 1
            db_close(db)
            time.sleep(count/2)
            db = db_connect()
    return success

def db_deployment_update_infra(db, infra_id):
    """
    Update deployment with IM infra id
    """
    try:
        cursor = db.cursor()
        cursor.execute('UPDATE deployments SET status="creating" WHERE id="%s"' % infra_id)
        db.commit()
    except Exception as e:
        logger.critical('[db_deployment_update_infra] Unable to connect to sqlite DB because of %s', e)
        return False
    return True

def db_deployment_update_status_with_retries(db, infra_id, status, cloud=None, im_infra_id=None):
    """
    Update deployment status with retries
    """
    max_retries = 10
    count = 0
    success = False
    while count < max_retries and not success:
        success = db_deployment_update_status(db, infra_id, status, cloud, im_infra_id)
        if not success:
            count += 1
            db_close(db)
            time.sleep(count/2)
            db = db_connect()
    return success

def db_deployment_update_status(db, id, status, cloud=None, im_infra_id=None):
    """
    Update deployment status
    """
    try:
        cursor = db.cursor()
        if cloud is not None and im_infra_id is not None:
            cursor.execute('UPDATE deployments SET status="%s",cloud="%s",im_infra_id="%s" WHERE id="%s"' % (status, cloud, im_infra_id, id))
        elif cloud is not None:
            cursor.execute('UPDATE deployments SET status="%s",cloud="%s" WHERE id="%s"' % (status, cloud, id))
        else:
            cursor.execute('UPDATE deployments SET status="%s" WHERE id="%s"' % (status, id))
        db.commit()
    except Exception as e:
        logger.critical('[db_deployment_update_status] Unable to connect to sqlite DB because of %s', e)
        return False
    return True

def db_set_token(cloud, token, expiry, creation):
    """
    Write token to the DB
    """
    try:
        db = sqlite3.connect(CONFIG.get('ansible', 'db'))
        cursor = db.cursor()
        cursor.execute('INSERT INTO credentials (cloud, token, expiry, creation) VALUES ("%s", "%s", %d, %d)' % (cloud, token, expiry, creation))
        db.commit()
        db.close()
    except Exception as e:
        logger.critical('[db_set] Unable to connect to sqlite DB because of %s', e)
        return False
    return True

def db_set_ansible_node(cloud, infrastructure_id, public_ip, username):
    """
    Write Ansible node details to DB
    """
    try:
        db = sqlite3.connect(CONFIG.get('ansible', 'db'))
        cursor = db.cursor()
        cursor.execute('INSERT INTO ansible_nodes (cloud, infrastructure_id, public_ip, username) VALUES ("%s", "%s", "%s", "%s")' % (cloud, infrastructure_id, public_ip, username))
        db.commit()
        db.close()
    except Exception as e:
        logger.critical('[db_set] Unable to connect to sqlite DB because of %s', e)
        return False
    return True

def db_get_ansible_node(cloud):
    """
    Get details about an Ansible node for the specified cloud
    """
    infrastructure_id = None
    public_ip = None
    username = None
    timestamp = None

    try:
        db = sqlite3.connect(CONFIG.get('ansible', 'db'))
        cursor = db.cursor()
        cursor.execute('SELECT infrastructure_id, public_ip, username, creation FROM ansible_nodes WHERE cloud="%s"' % cloud)

        for row in cursor:
            infrastructure_id = row[0]
            public_ip = row[1]
            username = row[2]
            timestamp = row[3]

        db.close()

    except Exception as e:
        logger.critical('[db_get] Unable to connect to sqlite DB because of %s', e)

    return (infrastructure_id, public_ip, username, timestamp)

def db_get_token(cloud):
    """
    Get a token & expiry date for the specified cloud
    """
    token = None
    expiry = -1
    creation = -1

    try:
        db = sqlite3.connect(CONFIG.get('ansible', 'db'))
        cursor = db.cursor()
        cursor.execute('SELECT token,expiry,creation FROM credentials WHERE cloud="%s"' % cloud)

        for row in cursor:
            token = row[0]
            expiry = row[1]
            creation = row[2]

        db.close()

    except Exception as e:
        logger.critical('[db_get] Unable to connect to sqlite DB because of %s', e)
        return (token, expiry, creation)

    return (token, expiry, creation)

def db_delete_ansible_node(cloud):
    """
    Delete an Ansible node for the specified cloud
    """
    try:
        db = sqlite3.connect(CONFIG.get('ansible', 'db'))
        cursor = db.cursor()
        cursor.execute('DELETE FROM ansible_nodes WHERE cloud="%s"' % cloud)
        db.commit()
        db.close()
    except Exception as e:
        logger.critical('[db_delete] Unable to connect to sqlite DB because of %s', e)
        return False
    return True

def db_delete_token(cloud):
    """
    Delete a token for the specified cloud
    """
    try:
        db = sqlite3.connect(CONFIG.get('ansible', 'db'))
        cursor = db.cursor()
        cursor.execute('DELETE FROM credentials WHERE cloud="%s"' % cloud)
        db.commit()
        db.close()
    except Exception as e:
        logger.critical('[db_delete] Unable to connect to sqlite DB because of %s', e)
        return False
    return True

def delete_ansible_node(cloud):
    """
    Delete an Ansible node for the specified cloud
    """
    # Get details about the node
    (infrastructure_id, public_ip, username, timestamp) = db_get_ansible_node(cloud)

    if infrastructure_id is None:
        logger.critical('[delete_ansible_node] Unable to get infrastructure id for Ansible node in cloud %s', cloud)
        return False

    logger.info('[delete_ansible_node] About to delete Ansible node from clouds %s with infrastructure id %s', cloud, infrastructure_id)

    #  Get a token if necessary
    token = get_token(cloud)

    # Destroy infrastructure
    im_auth = create_im_auth(cloud, token)
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return False
    (return_code, msg) = client.destroy(infrastructure_id, int(CONFIG.get('timeouts', 'deletion')))
    if return_code != 0:
        logger.critical('Unable to destroy Ansible node infrastructure with id "%s" on cloud "%s" due to "%s"', infrastructure_id, cloud, msg)

    # Delete from the DB
    db_delete_ansible_node(cloud)

    return True

def setup_ansible_node(cloud):
    """
    Find or create an Ansible node inside the specified cloud
    """

    # Check if there is a static Ansible node
    (ip_addr, username) = get_static_ansible_node(cloud)

    if ip_addr is not None and username is not None:
        logger.info('Found static Ansible node with ip_addr %s on cloud %s', ip_addr, cloud)
        status = check_ansible_node(ip_addr, username)
        if status:
            logger.info('Successfully tested Ansible node with ip %s on cloud %s', ip_addr, cloud)
            return (ip_addr, username)
        else:
            logger.info('Ansible node with ip %s on cloud %s not accessible', ip_addr, cloud)
    logger.info('No functional static Ansible node found for cloud %s', cloud)

    # Check if there is a dynamic Ansible node
    (ip_addr, username) = get_dynamic_ansible_node(cloud)

    if ip_addr is not None and username is not None:
        logger.info('Found existing dynamic Ansible node with ip %s on cloud %s', ip_addr, cloud)
        status = check_ansible_node(ip_addr, username)
        if status:
            logger.info('Successfully tested Ansible node with ip %s on cloud %s', ip_addr, cloud)
            return (ip_addr, username)
        else:
            logger.info('Ansible node with ip %s on cloud %s not accessible, so deleting', ip_addr, cloud)
            delete_ansible_node(cloud)
    logger.info('No functional dynamic Ansible node found for cloud %s', cloud)

    # Try to create a dynamic Ansible node
    infrastructure_id = deploy_ansible_node(cloud)

    if infrastructure_id is None:
        logger.critical('Unable to create Ansible node on cloud "%s"', cloud)
        return (None, None)

    # Get the public IP
    ip_addr = get_public_ip(infrastructure_id)

    if ip_addr is None:
        logger.critical('Newly deployed Ansible node has no public IP')
        return (None, None)

    # Set DB
    db_set_ansible_node(cloud, infrastructure_id, ip_addr, 'cloudadm')

    return (ip_addr, 'cloudadm')

def get_dynamic_ansible_node(cloud):
    """
    Check if the given cloud has a dynamic Ansible node and return its details if it does
    """
    (infrastructure_id, public_ip, username, timestamp) = db_get_ansible_node(cloud)
    return (public_ip, username)

def get_static_ansible_node(cloud):
    """
    Check if the given cloud has a static Ansible node and return it's details if it does
    """
    try:
        with open('/etc/prominence/imc.json') as file:
            data = json.load(file)
    except Exception as ex:
        logger.critical('Unable to open file containing static Ansible nodes due to: %s', ex)
        return (None, None)


    if 'ansible' in data:
        if cloud in data['ansible']:
            return (data['ansible'][cloud]['public_ip'], data['ansible'][cloud]['username'])

    return (None, None)

def get_public_ip(infrastructure_id):
    """
    Get the public IP of infrastructure
    """
    public_ip = None

    # Setup Infrastructure Manager client
    im_auth = create_im_auth(cloud, None)
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

def set_status(cloud, state):
    """
    Write cloud status information
    """
    data = {}
    data['epoch'] = time.time()

    try:
        response = requests.put('%s/v1/data/status/failures/%s' % (OPA_URL, cloud), json=data, timeout=OPA_TIMEOUT)
    except requests.exceptions.RequestException as e:
        logger.warning('Unable to write cloud status to Open Policy Agent due to "%s"', e)

def get_clouds(data):
    """
    Get list of clouds meeting requirements
    """
    data = {'input':data}

    try:
        response = requests.post('%s/v1/data/imc/sites' % OPA_URL, json=data, timeout=OPA_TIMEOUT)
    except requests.exceptions.RequestException as e:
        logger.critical('Unable to get list of sites from Open Policy Agent due to "%s"', e)
        return []

    if 'result' in response.json():
        return response.json()['result']

    return []

def get_ranked_clouds(data, clouds):
    """
    Get list of ranked clouds based on preferences
    """
    data = {'input':data}
    data['input']['clouds'] = clouds

    try:
        response = requests.post('%s/v1/data/imc/rankedsites' % OPA_URL, json=data, timeout=OPA_TIMEOUT)
    except requests.exceptions.RequestException as e:
        logger.critical('Unable to get list of ranked sites from Open Policy Agent due to "%s"', e)
        return []

    if 'result' in response.json():
        return response.json()['result']

    return []

def get_image(data, cloud):
    """
    Get name of an image at the specified site meeting any given requirements
    """
    data = {'input':data}
    data['input']['cloud'] = cloud

    try:
        response = requests.post('%s/v1/data/imc/images' % OPA_URL, json=data, timeout=OPA_TIMEOUT)
    except requests.exceptions.RequestException as e:
        logger.critical('Unable to get image from Open Policy Agent due to "%s"', e)
        return None

    if 'result' in response.json():
        if len(response.json()['result']) > 0:
            return response.json()['result'][0]

    return None

def get_flavour(data, cloud):
    """
    Get name of a flavour at the specified site meeting requirements. We are given a list
    of flavours and weights, and we pick the flavour with the lowest weight.
    """
    data = {'input':data}
    data['input']['cloud'] = cloud

    try:
        response = requests.post('%s/v1/data/imc/flavours' % OPA_URL, json=data, timeout=OPA_TIMEOUT)
    except requests.exceptions.RequestException as e:
        logger.critical('Unable to get flavour from Open Policy Agent due to "%s"', e)
        return None

    flavour = None
    if 'result' in response.json():
        if len(response.json()['result']) > 0:
            flavours = sorted(response.json()['result'], key=lambda k: k['weight'])
            flavour = flavours[0]['name']

    return flavour

def deploy_ansible_node(cloud):
    """
    Deploy an Ansible node with public IP address
    """
    logger.info('Deploying Ansible node for cloud "%s"', cloud)

    # Open RADL template file
    try:
        with open(CONFIG.get('ansible', 'template')) as data:
            radl_t = Template(data.read())
    except IOError:
        logger.critical('Unable to open RADL template for Ansible node from file "%s"', filename)
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

    # Generate JSON to be given to Open Policy Agent
    userdata = {'requirements':requirements, 'preferences':{}}

    # Get the image & flavour
    image = get_image(userdata, cloud)
    flavour = get_flavour(userdata, cloud)

    if image is None:
        logger.critical('Unable to deploy Ansible node because no acceptable image is available')
        return None

    if flavour is None:
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

    # Write RADL to temporary file
    file_desc, path = tempfile.mkstemp()
    try:
        with os.fdopen(file_desc, 'w') as tmp:
            tmp.write(radl)
    except:
        logger.critical('Error writing RADL file for Ansible node')
        return None

    time_begin = time.time()

    # Deploy infrastructure
    infra_id = deploy(path, cloud, time_begin, None)

    # Remove temporary RADL file
    os.remove(path)

    return infra_id

def deploy_job(db, radl_contents, requirements, preferences, unique_id, dryrun):
    """
    Find an appropriate cloud to deploy infrastructure
    """
    radls = {}
    radlst = {}

    radls['default'] = radl_contents
    radlst['default'] = Template(radl_contents)

    # Count number of instances
    instances = 0
    for line in radls['default'].split('\n'):
        m = re.search(r'deploy.*(\d+)', line)
        if m:
            instances += int(m.group(1))
    logger.info('Found %d instances to deploy', instances)
    requirements['resources']['instances'] = instances

    # Generate JSON to be given to Open Policy Agent
    userdata = {'requirements':requirements, 'preferences':preferences}

    # Get list of clouds meeting the specified requirements
    clouds = get_clouds(userdata)
    logger.info('Suitable clouds = [%s]', ','.join(clouds))

    if not clouds:
        logger.critical('No clouds exist which meet the requested requirements')
        return False

    # Update dynamic information about each cloud if necessary

    # Shuffle list of clouds
    shuffle(clouds)

    # Rank clouds as needed
    clouds_ranked = get_ranked_clouds(userdata, clouds)
    clouds_ranked_list = []
    for item in sorted(clouds_ranked, key=lambda k: k['weight'], reverse=True):
        clouds_ranked_list.append(item['site'])
    logger.info('Ranked clouds = [%s]', ','.join(clouds_ranked_list))

    # Update status
    db_deployment_update_infra_with_retries(db, unique_id)

    # Try to create infrastructure, exiting on the first successful attempt
    time_begin = time.time()
    success = False

    for item in sorted(clouds_ranked, key=lambda k: k['weight'], reverse=True):
        infra_id = None
        cloud = item['site']
        image = get_image(userdata, cloud)
        flavour = get_flavour(userdata, cloud)
        logger.info('Attempting to deploy on cloud "%s" with image "%s" and flavour "%s"', cloud, image, flavour)

        if flavour is None:
            logger.info('Skipping because no flavour could be determined')
            continue

        if image is None:
            logger.info('Skipping because no image could be determined')
            continue

        # Get correct RADL template for this cloud
        radl_used = None
        if cloud in radlst:
            radl_t = radlst[cloud]
            radl_used = cloud
        else:
            radl_t = radlst['default']
            radl_used = 'default'
        logger.info('Using RADL template "%s"', radl_used)

        if dryrun:
            continue

        # Setup Ansible node if necessary
        if requirements['resources']['instances'] > 1 and 'Google' not in cloud:
            (ip_addr, username) = setup_ansible_node(cloud)
            if ip_addr is None or username is None:
                logger.critical('Unable to find existing or create an Ansible node in cloud %s because ip=%s,username=%s', cloud, ip_addr, username)
                continue
            logger.info('Ansible node in cloud %s available, now will deploy infrastructure for the job', cloud)
        else:
            logger.info('Ansible node not required')
            ip_addr = None
            username = None

        # Get the private key
        try:
            with open(CONFIG.get('ansible', 'private_key')) as data:
                private_key = data.read()
        except IOError:
            logger.critical('Unable to open private key for Ansible node from file "%s"', filename)
            return False


        # Create complete RADL content
        try:
            radl = radl_t.substitute(instance=flavour,
                                     image=image,
                                     cloud=cloud,
                                     ansible_ip=ip_addr,
                                     ansible_username=username,
                                     ansible_private_key=private_key
                                     )
        except Exception as e:
            logger.critical('Error creating RADL from template due to %s', e)
            return False

        # Write RADL to temporary file
        file_desc, path = tempfile.mkstemp()
        try:
            with os.fdopen(file_desc, 'w') as tmp:
                tmp.write(radl)
        except:
            logger.critical('Error writing RADL file')
            return False

        # Deploy infrastructure
        infra_id = deploy(path, cloud, time_begin, unique_id)

        # Remove temporary RADL file
        os.remove(path)

        if infra_id is not None:
            success = True
            if unique_id is not None:
                print('Success deployment',unique_id,cloud)
                db_deployment_update_status_with_retries(db, unique_id, 'configured', cloud, infra_id)
            break

    if unique_id is not None and infra_id is None:
        db_deployment_update_status_with_retries(db, unique_id, 'failed', 'none', 'none')
    return success

def deploy(path, cloud, time_begin, unique_id):
    """
    Deploy infrastructure from a specified RADL file
    """
    # Check & get auth token if necessary
    token = get_token(cloud)

    # Setup Infrastructure Manager client
    im_auth = create_im_auth(cloud, token)
    client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
    (status, msg) = client.getauth()
    if status != 0:
        logger.critical('Error reading IM auth file: %s', msg)
        return None

    # Retry loop
    retries_per_cloud = int(CONFIG.get('deployment', 'retries'))
    retry = 0
    success = False
    while retry < retries_per_cloud + 1 and success is not True:
        if retry > 0:
            time.sleep(int(CONFIG.get('polling', 'duration')))
        logger.info('Deployment attempt %d of %d', retry+1, retries_per_cloud+1)
        retry += 1

        # Create infrastructure
        start = time.time()
        (infrastructure_id, msg) = client.create(path, int(CONFIG.get('timeouts', 'creation')))
        duration = time.time() - start
        logger.info('Duration of create request %d s on cloud %s', duration, cloud)

        if infrastructure_id is not None:
            logger.info('Created infrastructure with id "%s" on cloud "%s" for id "%s" and waiting for it to be configured', infrastructure_id, cloud, unique_id)
            #if unique_id is not None:
            #    db_deployment_update_infra(db, unique_id, infrastructure_id)

            # Wait for infrastructure to enter the configured state
            time_created = time.time()
            count_unconfigured = 0
            state_previous = None

            while True:
                # Don't spend too long trying to create infrastructure, give up eventually
                if time.time() - time_begin > int(CONFIG.get('timeouts', 'total')):
                    logger.info('Giving up, waiting too long so will destroy infrastructure with id "%s"', infrastructure_id)
                    destroy(client, infrastructure_id, cloud)
                    return None

                time.sleep(int(CONFIG.get('polling', 'duration')))
                (state, msg) = client.getstate(infrastructure_id, int(CONFIG.get('timeouts', 'status')))

                # Handle situation in which the cloud state cannot be determined
                if state is None:
                    logger.info('Unable to determine current state of infrastructure with id "%s" on cloud "%s"', infrastructure_id, cloud)
                    continue

                # Log a change in state
                if state != state_previous:
                    logger.info('Infrastructure with our id "%s" and IM id "%s" is in state %s', unique_id, infrastructure_id, state)
                    state_previous = state

                # Handle configured state
                if state == 'configured':
                    logger.info('Successfully configured infrastructure with our id "%s" on cloud "%s"', infrastructure_id, cloud)
                    success = True
                    return infrastructure_id

                # Destroy infrastructure which is taking too long to enter the configured state
                if time.time() - time_created > int(CONFIG.get('timeouts', 'configured')):
                    logger.warning('Waiting too long for infrastructure to be configured, so destroying')
                    set_status(cloud, 'configuration-too-long')
                    destroy(client, infrastructure_id, cloud)
                    break

                # Destroy infrastructure which is taking too long to enter the running state
                if time.time() - time_created > int(CONFIG.get('timeouts', 'notrunning')) and state != 'running' and state != 'unconfigured':
                    logger.warning('Waiting too long for infrastructure to enter the running state, so destroying')
                    set_status(cloud, 'pending-too-long')
                    destroy(client, infrastructure_id, cloud)
                    break

                # Destroy infrastructure for which deployment failed
                if state == 'failed':
                    logger.warning('Infrastructure creation failed, so destroying; error was "%s"', msg)
                    set_status(cloud, state)
                    destroy(client, infrastructure_id, cloud)
                    break

                # Handle unconfigured infrastructure
                if state == 'unconfigured':
                    count_unconfigured += 1
                    file_unconf = '/tmp/contmsg-%s-%d.txt' % (unique_id, time.time())
                    contmsg = client.getcontmsg(infrastructure_id, int(CONFIG.get('timeouts', 'deletion')))
                    if count_unconfigured == 1:
                        logger.warning('Infrastructure is unconfigured, will try reconfiguring once after writing contmsg to a file')
                        with open(file_unconf, 'w') as unconf:
                            unconf.write(contmsg)
                        client.reconfigure(infrastructure_id, int(CONFIG.get('timeouts', 'reconfigure')))
                    else:
                        logger.warning('Infrastructure has been unconfigured too many times, so destroying after writing contmsg to a file')
                        set_status(cloud, state)
                        with open(file_unconf, 'w') as unconf:
                            unconf.write(contmsg)
                        destroy(client, infrastructure_id, cloud)
                        break
        else:
            logger.warning('Deployment failure on cloud "%s" for infrastructure with id "%s" with msg="%s"', cloud, infrastructure_id, msg)
            if msg == 'timedout':
                logger.warning('Infrastructure creation failed due to a timeout')
                set_status(cloud, 'creation-timeout')
            else:
                file_failed = '/tmp/failed-%s-%d.txt' % (unique_id, time.time())
                set_status(cloud, 'creation-failed')
                logger.warning('Infrastructure creation failed, writing stdout/err to file "%s"', file_failed)
                with open(file_failed, 'w') as failed:
                    failed.write(msg)

    return None

def imc_delete(unique_id):
    """
    Delete the infrastructure with the specified id - wrapper
    """
    try:
        imc_delete_(unique_id)
    except Exception as ex:
        logger.critical('Exception deleting infrastructure: "%s"' % ex)
    return

def imc_delete_(unique_id):
    """
    Delete the infrastructure with the specified id
    """
    db = db_connect()
    logger.info('Deleting infrastructure "%s"', unique_id)
    db_deployment_update_status_with_retries(db, unique_id, 'deleting')

    (im_infra_id, infra_status, cloud) = db_deployment_get_im_infra_id(db, unique_id)

    if im_infra_id is not None and cloud is not None:
        match_obj_name = re.match(r'\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b', im_infra_id)
        if match_obj_name:
            logger.info('Deleting IM infrastructure with id "%s"', im_infra_id)
            # Check & get auth token if necessary
            token = get_token(cloud)

            # Setup Infrastructure Manager client
            im_auth = create_im_auth(cloud, token)
            client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
            (status, msg) = client.getauth()
            if status != 0:
                logger.critical('Error reading IM auth file: %s', msg)
                db_close(db)
                return 1

            destroyed = destroy(client, im_infra_id, cloud)

            if destroyed:
                db_deployment_update_status_with_retries(db, unique_id, 'deleted')
                logger.info('Destroyed infrastructure "%s" with IM infrastructure id "%s"', unique_id, im_infra_id)
            else:
                logger.info('Unable to destroy infrastructure "%s" with IM infrastructure id "%s"', unique_id, im_infra_id)
    else:
        logger.info('No need to destroy infrastructure because IM infrastructure id is "%s" and cloud is "%s"', im_infra_id, cloud)
    db_close(db)
    return 0

def imc_status(unique_id):
    """
    Return the status of the infrastructure from the specified id
    """
    db = db_connect()
    (im_infra_id, status, cloud) = db_deployment_get_im_infra_id(db, unique_id)
    db_close(db)
    return (im_infra_id, status, cloud)

def imc_deploy(inputj, unique_id):
    """
    Deploy infrastructure given a JSON specification and id - wrapper
    """
    try:
        imc_deploy_(inputj, unique_id)
    except Exception as ex:
        logger.critical('Exception deploying infrastructure: "%s"' % ex)
    return

def imc_deploy_(inputj, unique_id):
    """
    Deploy infrastructure given a JSON specification and id
    """
    dryrun = False
    logger.info('Deploying infrastructure with id %s', unique_id)

    db = db_connect()

    # Update DB
    success = db_deployment_create_with_retries(db, unique_id)
    if not success:
        logger.critical('Unable to update DB so unable to deploy infrastructure')
        return 1

    # Generate requirements & preferences
    if 'preferences' in inputj:
        preferences_new = {}
        # Generate list of weighted regions if necessary
        if 'regions' in inputj['preferences']:
            preferences_new['regions'] = {}
            for i in range(0, len(inputj['preferences']['regions'])):
                preferences_new['regions'][inputj['preferences']['regions'][i]] = len(inputj['preferences']['regions']) - i
        # Generate list of weighted sites if necessary
        if 'sites' in inputj['preferences']:
            preferences_new['sites'] = {}
            for i in range(0, len(inputj['preferences']['sites'])):
                preferences_new['sites'][inputj['preferences']['sites'][i]] = len(inputj['preferences']['sites']) - i
        inputj['preferences'] = preferences_new

    if 'requirements' in inputj:
        requirements = inputj['requirements']
        preferences = inputj['preferences']

    if 'radl' in inputj:
        radl_contents = base64.b64decode(inputj['radl'])

    # Attempt to deploy infrastructure
    success = deploy_job(db, radl_contents, requirements, preferences, unique_id, dryrun)

    if not success:
        db_deployment_update_status_with_retries(db, unique_id, 'unable')

    db_close(db)

    if not success:
        logger.critical('Unable to deploy infrastructure on any cloud')
        return 1

    return 0

if __name__ == "__main__":
    app.run()
