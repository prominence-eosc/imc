#!/usr/bin/python

from __future__ import print_function
from concurrent.futures import ProcessPoolExecutor
import os
import uuid
import ConfigParser
from flask import Flask, request, jsonify

import database
import imc

app = Flask(__name__)

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
            executor.submit(imc.infrastructure_deploy, request.get_json(), uid)
            db.close()
            return jsonify({'id':uid}), 201
    return jsonify({'id':uid}), 400

@app.route('/infrastructures/<string:infra_id>', methods=['GET'])
def get_infrastructure(infra_id):
    """
    Get current status of specified infrastructure
    """
    (im_infra_id, status, cloud) = imc.infrastructure_status(infra_id)
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
        success = db.deployment_update_status_with_retries(infra_id, 'deleting')
        if success:
            executor.submit(imc.infrastructure_delete, infra_id)
            db.close()
            return jsonify({'id':uid}), 200
    return jsonify({'id':uid}), 400

if __name__ == "__main__":
    app.run()
