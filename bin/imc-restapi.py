#!/usr/bin/env python
"""REST API"""

from __future__ import print_function
from functools import wraps
import uuid
import logging
from logging.handlers import RotatingFileHandler
from flask import Flask, request, jsonify

from imc import config
from imc import database
from imc import imclient
from imc import logger as custom_logger
from imc import return_sites
from imc import tokens
from imc import cloud_utils
from imc import utilities
from imc import health

# Configuration
CONFIG = config.get_config()

# Setup handlers for the root logger
handler = RotatingFileHandler(CONFIG.get('logs', 'filename').replace('.log', '-restapi.log'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Create flask application
app = Flask(__name__)

# Initialize DB if necessary
dbi = database.get_db()
dbi.init()

def authenticate():
    """
    Sends a 401 response
    """
    return jsonify({'error':'Authentication failure'}), 401

def valid_credentials(username, password):
    """
    Check if the supplied credentials are valid
    """
    return username == CONFIG.get('auth', 'username') and password == CONFIG.get('auth', 'password')

def requires_auth(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        auth = request.authorization
        if not auth:
            return authenticate()
        if not auth.username or not auth.password or not valid_credentials(auth.username, auth.password):
            return authenticate()
        return function(*args, **kwargs)
    return wrapper

@app.route('/infrastructures', methods=['POST'])
@requires_auth
def create_infrastructure():
    """
    Create infrastructure
    """
    using_idempotency_key = False

    if request.headers.get('Idempotency-Key'):
        uid = request.headers.get('Idempotency-Key')
        using_idempotency_key = True
        if not utilities.valid_uuid(uid):
            uid = str(uuid.uuid4())
    else:
        uid = str(uuid.uuid4())
    logger = custom_logger.CustomAdapter(logging.getLogger(__name__), {'id': uid})

    if using_idempotency_key:
        logger.info('Using Idempotency-Key as infra id')

    identity = None
    if 'identity' in request.get_json():
        identity = request.get_json()['identity']

    identifier = None
    if 'identifier' in request.get_json():
        identifier = request.get_json()['identifier']

    if 'dryrun' in request.get_json():
        sites = return_sites.return_sites(request.get_json())
        return jsonify({'sites':sites}), 200

    db = database.get_db()
    if db.connect():
        check = db.deployment_check_infra_id(uid)
        if check == 1:
            success = db.deployment_create_with_retries(uid, request.get_json(), identity, identifier)
            db.close()
            if success:
                logger.info('Infrastructure creation request successfully initiated')
                return jsonify({'id':uid}), 201
        elif check == 0:
            db.close()
            logger.info('Duplicate Idempotency-Key used')
            return jsonify({'id':uid}), 200
        else:
            db.close()
            logger.critical('Unable to check if infrastructure ID was already used')
    logger.critical('Infrastructure creation request failed, possibly a database issue')
    return jsonify({'id':uid}), 400

@app.route('/infrastructures/', methods=['GET'])
@requires_auth
def get_infrastructures():
    """
    Get list of infrastructures in the specified state or type
    """
    if 'status' in request.args and 'type' not in request.args:
        cloud = None
        if 'cloud' in request.args:
            cloud = request.args.get('cloud')
        db = database.get_db()
        if db.connect():
            infra = db.deployment_get_infra_in_state_cloud(request.args.get('status'), cloud)
            db.close()
            return jsonify(infra), 200
    elif 'type' in request.args and 'cloud' in request.args:
        if request.args.get('type') == 'im':
            cloud = request.args.get('cloud')
            db = database.get_db()
            if db.connect():
                clouds_info_list = cloud_utils.create_clouds_list(db, identity)
                token = tokens.get_token(cloud, None, db, clouds_info_list)
                db.close()
                im_auth = utilities.create_im_auth(cloud, token, clouds_info_list)
                client = imclient.IMClient(url=CONFIG.get('im', 'url'), data=im_auth)
                (status, msg) = client.getauth()
                if status != 0:
                    logger.critical('Error reading IM auth file: %s', msg)
                    return jsonify({}), 400
                (status, ids) = client.list_infra_ids(10)
                im_list = []
                if ids:
                    for uri in ids:
                        pieces = uri.split('/')
                        im_id = pieces[len(pieces) - 1]
                        im_list.append(im_id)
                    return jsonify(im_list), 200
        
    return jsonify({}), 400

@app.route('/health', methods=['GET'])
def get_health():
    """
    Get the current health
    """
    (status, msg) = health.health()
    if not status:
        return jsonify({'error': msg}), 409

    return jsonify({}), 204

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

    db = database.get_db()
    if db.connect():
        (im_infra_id, status, cloud, _, _) = db.deployment_get_im_infra_id(infra_id)
        if status in ('unable', 'failed', 'waiting'):
            status_reason = db.deployment_get_status_reason(infra_id)
    db.close()
    if status:
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
        db = database.get_db()
        if db.connect():
            # Get current status of infrastructure
            (_, status, _, _, _) = db.deployment_get_im_infra_id(infra_id)

            # If it has already been deleted, don't do anything but return success
            if status == 'deleted':
                db.close()
                logger.info('Infrastructure has already been deleted')
                return jsonify({}), 200
            elif status == 'deletion-requested':
                db.close()
                logger.info('Infrastructure deletion has already been requested')
                return jsonify({}), 200

            success = db.deployment_update_status(infra_id, 'deletion-requested')
            if success:
                db.close()
                logger.info('Infrastructure deletion request successfully initiated')
                return jsonify({}), 200
        logger.critical('Infrastructure deletion request failed, possibly a database issue')
        return jsonify({}), 400
    elif request.args.get('type') == 'im':
        cloud = request.args.get('cloud')
        db = database.get_db()
        if db.connect():
            clouds_info_list = utilities.create_clouds_list(CONFIG.get('clouds', 'path'))
            token = tokens.get_token(cloud, None, db, clouds_info_list)
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

@app.route('/credentials', methods=['POST'])
@requires_auth
def create_user_credentials():
    """
    Insert or update user credentials
    """
    username = None
    refresh_token = None

    data = request.get_json()
    if 'username' in data:
        username = data['username']
    if 'refresh_token' in data:
        refresh_token = data['refresh_token']

    if not username or not refresh_token:
        return jsonify({'error':'json data not valid'}), 400

    db = database.get_db()
    if db.connect():
        status = db.set_user_credentials(username, refresh_token)
        db.close()
        if status:
            return jsonify({}), 201
    return jsonify({}), 400

if __name__ == "__main__":
    app.run()

