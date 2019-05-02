#!/usr/bin/python

from __future__ import print_function
from concurrent.futures import ProcessPoolExecutor
import os
import sys
import uuid
import ConfigParser
import logging
from flask import Flask, request, jsonify

import database
import imc

app = Flask(__name__)

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

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
dbi = database.Database(CONFIG.get('db', 'host'),
                        CONFIG.get('db', 'port'),
                        CONFIG.get('db', 'db'),
                        CONFIG.get('db', 'username'),
                        CONFIG.get('db', 'password'))

dbi.init()

def infrastructure_deploy(input_json, unique_id):
    """
    Deploy infrastructure given a JSON specification and id
    """
    try:
        imc.auto_deploy(input_json, unique_id)
    except Exception as error:
        logger.critical('Exception deploying infrastructure: "%s"', error)
    return

def infrastructure_delete(unique_id):
    """
    Delete the infrastructure with the specified id
    """
    try:
        imc.delete(unique_id)
    except Exception as error:
        logger.critical('Exception deleting infrastructure: "%s"', error)
    return

@app.route('/infrastructures', methods=['POST'])
def create_infrastructure():
    """
    Create infrastructure
    """
    uid = str(uuid.uuid4())

    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))

    if db.connect():
        success = db.deployment_create_with_retries(uid)
        if success:
            db.close()
            executor.submit(infrastructure_deploy, request.get_json(), uid)
            logger.info('Infrastructure creation request successfully initiated with id %s', uid)
            return jsonify({'id':uid}), 201
    logger.critical('Infrastructure creation request with id %s failed, possibly a database issue', uid)
    return jsonify({'id':uid}), 400

@app.route('/infrastructures/<string:infra_id>', methods=['GET'])
def get_infrastructure(infra_id):
    """
    Get current status of specified infrastructure
    """
    logger.info('Infrastructure status request for id %s', infra_id)
    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))

    im_infra_id = None
    status = None
    cloud = None

    if db.connect():
        (im_infra_id, status, cloud) = db.deployment_get_im_infra_id(unique_id)
    db.close()
    if status is not None:
        return jsonify({'status':status, 'cloud':cloud, 'infra_id':im_infra_id}), 200
    return jsonify({'status':'invalid'}), 404

@app.route('/infrastructures/<string:infra_id>', methods=['DELETE'])
def delete_infrastructure(infra_id):
    """
    Delete the specified infrastructure
    """
    db = database.Database(CONFIG.get('db', 'host'),
                           CONFIG.get('db', 'port'),
                           CONFIG.get('db', 'db'),
                           CONFIG.get('db', 'username'),
                           CONFIG.get('db', 'password'))

    if db.connect():
        success = db.deployment_update_status_with_retries(infra_id, 'deletion-requested')
        if success:
            db.close()
            executor.submit(infrastructure_delete, infra_id)
            logger.info('Infrastructure deletion request successfully initiated with id %s', infra_id)
            return jsonify({}), 200
    logger.critical('Infrastructure deletion request with id %s failed, possibly a database issue', infra_id)
    return jsonify({}), 400

if __name__ == "__main__":
    app.run()
