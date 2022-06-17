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
    data['requirements']['image']['distribution'] = CONFIG.get('workers.image', 'distribution')
    data['requirements']['image']['version'] = CONFIG.get('workers.image', 'version')
    data['requirements']['image']['type'] = CONFIG.get('workers.image', 'type')
    data['requirements']['image']['architecture'] = CONFIG.get('workers.image', 'architecture')
    data['requirements']['resources'] = {}
    data['requirements']['resources']['cores'] = cpus
    data['requirements']['resources']['memory'] = memory
    data['requirements']['resources']['disk'] = disk
    data['requirements']['resources']['instances'] = nodes

    logger.info('Will deploy node with cpus=%d, memory=%dGB, disk=%dGB for job %d', cpus, memory, disk, job['id'])

    data['requirements']['regions'] = []
    data['requirements']['sites'] = CONFIG.get('workers.placement', 'sites_requirements').split(',')
    data['preferences'] = {}
    data['preferences']['regions'] = []
    data['preferences']['sites'] = []
   
    return data

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

