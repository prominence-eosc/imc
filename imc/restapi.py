"""REST API"""

from __future__ import print_function
from concurrent.futures import ProcessPoolExecutor
from functools import wraps
import os
import re
import sys
import uuid
import ConfigParser
import logging
from flask import Flask, request, jsonify

import consistency
import database
import imc
import imclient
import logger as custom_logger
import tokens
import utilities

def get_db():
    """
    Prepare DB
    """
    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))
    return db

app = Flask(__name__)

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')

# Configuration
CONFIG = ConfigParser.ConfigParser()
if 'PROMINENCE_IMC_CONFIG_DIR' in os.environ:
    CONFIG.read('%s/imc.ini' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
else:
    print('ERROR: Environment variable PROMINENCE_IMC_CONFIG_DIR has not been defined')
    exit(1)

# Setup process pool for handling deployments
executor = ProcessPoolExecutor(int(CONFIG.get('pool', 'size')))

# Initialize DB if necessary
dbi = get_db()
dbi.init()

def infrastructure_deploy(input_json, unique_id):
    """
    Deploy infrastructure given a JSON specification and id
    """
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': unique_id})
    try:
        imc.auto_deploy(input_json, unique_id)
    except Exception as error:
        logger.critical('Exception deploying infrastructure: "%s"', error)
    return

def infrastructure_delete(unique_id):
    """
    Delete the infrastructure with the specified id
    """
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': unique_id})
    try:
        imc.delete(unique_id)
    except Exception as error:
        logger.critical('Exception deleting infrastructure: "%s"', error)
    return

def authenticate():
    """
    Sends a 401 response
    """
    return jsonify({'error':'Authentication failure'}), 401

def check_token(token):
    """
    Check a token
    """
    if token == CONFIG.get('auth', 'token'):
        return True

    return False

def requires_auth(function):
    """
    Check authentication
    """
    @wraps(function)
    def decorated(*args, **kwargs):
        return function(*args, **kwargs)
        if 'Authorization' not in request.headers:
            return authenticate()
        auth = request.headers['Authorization']
        try:
            token = auth.split(' ')[1]
        except:
            return authenticate()
        if not check_token(token):
            return authenticate()
        return function(*args, **kwargs)
    return decorated

@app.route('/infrastructures', methods=['POST'])
@requires_auth
def create_infrastructure():
    """
    Create infrastructure
    """
    uid = str(uuid.uuid4())
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': uid})

    db = get_db()
    if db.connect():
        success = db.deployment_create_with_retries(uid)
        if success:
            db.close()
            executor.submit(infrastructure_deploy, request.get_json(), uid)
            logger.info('Infrastructure creation request successfully initiated')
            return jsonify({'id':uid}), 201
    logger.critical('Infrastructure creation request failed, possibly a database issue')
    return jsonify({'id':uid}), 400

@app.route('/infrastructures/', methods=['GET'])
@requires_auth
def get_infrastructures():
    """
    Get list of infrastructures in the specified state or type
    """
    if 'status' in request.args:
        cloud = None
        if 'cloud' in request.args:
            cloud = request.args.get('cloud')
        db = get_db()
        if db.connect():
            infra = db.deployment_get_infra_in_state_cloud(request.args.get('status'), cloud)
            db.close()
            return jsonify(infra), 200
    elif 'type' in request.args:
        if request.args.get('type') == 'im':
            consistency.find_unexpected_im_infras()
            return jsonify({}), 200
        
    return jsonify({}), 400

@app.route('/infrastructures/<string:infra_id>', methods=['GET'])
@requires_auth
def get_infrastructure(infra_id):
    """
    Get current status of specified infrastructure
    """
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': infra_id})
    logger.info('Infrastructure status request')

    im_infra_id = None
    status = None
    status_reason = None
    cloud = None

    db = get_db()
    if db.connect():
        (im_infra_id, status, cloud, _, _) = db.deployment_get_im_infra_id(infra_id)
        if status in ('unable', 'failed'):
            status_reason = db.deployment_get_status_reason(infra_id)
    db.close()
    if status is not None:
        return jsonify({'status':status, 'status_reason':status_reason, 'cloud':cloud, 'infra_id':im_infra_id}), 200
    return jsonify({'status':'invalid'}), 404

@app.route('/infrastructures/<string:infra_id>', methods=['DELETE'])
@requires_auth
def delete_infrastructure(infra_id):
    """
    Delete the specified infrastructure
    """
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': infra_id})

    if 'type' not in request.args:
        db = get_db()
        if db.connect():
            success = db.deployment_update_status_with_retries(infra_id, 'deletion-requested')
            if success:
                db.close()
                executor.submit(infrastructure_delete, infra_id)
                logger.info('Infrastructure deletion request successfully initiated')
                return jsonify({}), 200
        logger.critical('Infrastructure deletion request failed, possibly a database issue')
        return jsonify({}), 400
    elif request.args.get('type') == 'im':
        cloud = request.args.get('cloud')
        db = get_db()
        if db.connect():
            clouds_info_list = utilities.create_clouds_list(CONFIG.get('clouds', 'path'))
            token = tokens.get_token(cloud, db, clouds_info_list)
            db.close()
            im_auth = utilities.create_im_auth(cloud, token, clouds_info_list)
            client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
            (status, msg) = client.getauth()
            if status != 0:
                logger.critical('Error reading IM auth file: %s', msg)
                return jsonify({}), 400        
            client.destroy(infra_id, 30)
            return jsonify({}), 200
    return jsonify({}), 400

if __name__ == "__main__":
    app.run()
