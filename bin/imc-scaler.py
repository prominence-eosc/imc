#!/usr/bin/env python
"""PROMINENCE scaler"""

from functools import wraps
import time
import uuid
import logging
from logging.handlers import RotatingFileHandler

from imc import config
from imc import database
from imc import logger as custom_logger
from imc import jobs

# Configuration
CONFIG = config.get_config()

# Setup handlers for the root logger
handler = RotatingFileHandler(CONFIG.get('logs', 'filename').replace('.log', '-scaler.log'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Initialize DB if necessary
dbi = database.get_db()
dbi.init()

def create_json(job):
    """
    """
    cpus = job['cpus']
    memory = int(job['memory']/1024.0) + 1
    disk = int(job['disk']/1000.0/1000.0) + 10
    nodes = job['nodes']

    data = {}
    data['requirements'] = {}
    data['requirements']['image'] = {}
    data['requirements']['image']['distribution'] = 'centos'
    data['requirements']['image']['version'] = '7'
    data['requirements']['image']['type'] = 'linux'
    data['requirements']['image']['architecture'] = 'x86_64'
    data['requirements']['resources'] = {}
    data['requirements']['resources']['cores'] = cpus
    data['requirements']['resources']['memory'] = memory
    data['requirements']['resources']['disk'] = disk
    data['requirements']['resources']['instances'] = nodes

    logger.info('Will deploy node with cpus=%d, memory=%dGB, disk=%dGB for job %d', cpus, memory, disk, job['id'])

    data['requirements']['regions'] = []
    data['requirements']['sites'] = ['Azure-1']
    data['preferences'] = {}
    data['preferences']['regions'] = []
    data['preferences']['sites'] = []
   
    return data

def create_infrastructure(identity, identifier, data):
    """
    Create infrastructure
    """
    uid = str(uuid.uuid4())

    db = database.get_db()
    if db.connect():
        success = db.deployment_create(uid, data, identity, identifier)
        db.close()
        if success:
            logger.info('Infrastructure creation request successfully initiated')
            return uid
    return None

def get_infrastructures(status):
    """
    Get list of infrastructures in the specified status
    """
    db = database.get_db()
    if db.connect():
        infra = db.deployment_get_infra_in_state_cloud(status)
        db.close()
        return infra
        
    return None

def get_infrastructure(infra_id):
    """
    Get current status of specified infrastructure
    """
    resource_infra_id = None
    status = None
    status_reason = None
    cloud = None

    db = database.get_db()
    if db.connect():
        (resource_infra_id, status, cloud, _, _) = db.deployment_get_infra_id(infra_id)
        if status in ('unable', 'failed', 'waiting'):
            status_reason = db.deployment_get_status_reason(infra_id)
    db.close()
    if status:
        return {'status':status, 'status_reason':status_reason, 'cloud':cloud, 'infra_id':resource_infra_id}
    return None

def delete_infrastructure(infra_id):
    """
    Delete the specified infrastructure
    """
    db = database.get_db()
    if db.connect():
        success = db.deployment_update_status(infra_id, 'deletion-requested')
        if success:
            db.close()
            logger.info('Infrastructure deletion request successfully initiated')
            return True
    return None

def scaler(db):
    """
    Scaler
    """
    logger.info('Started scaling cycle')

    # Get jobs
    idle_jobs = jobs.get_idle_jobs()

    for job in idle_jobs:
        job_db = db.get_job(job['id'])

        if not job_db:
            data = create_json(job)
            success = db.deployment_create(str(uuid.uuid4()), data, job['identity'], job['id'])

            if not job_db:
                db.add_job(job['id'])
            else:
                db.update_job(job['id'], 0)

    logger.info('Ended scaling cycle')

if __name__ == "__main__":
    while True:
        logger.info('Connecting to the DB')
        db = database.get_db()
        if db.connect():
            scaler(db)
            db.close()
        else:
            logger.critical('Unable to connect to database')

        time.sleep(int(CONFIG.get('polling', 'scaler')))

