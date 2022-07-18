#!/usr/bin/env python
"""PROMINENCE scaler"""

from functools import wraps
import math
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
    Create json describing required worker
    """
    cpus = job['cpus']
    memory = int(job['memory']/1000.0)
    disk = int(job['disk']/1000.0/1000.0) + 10
    nodes = job['nodes']

    job_json = jobs.get_json(job['iwd'])

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

    if 'policies' in job_json:
        if 'placement' in job_json['policies']:
            if 'requirements' in job_json['policies']['placement']:
                if 'sites' in job_json['policies']['placement']['requirements']:
                    data['requirements']['sites'] = job_json['policies']['placement']['requirements']['sites']
                if 'regions' in job_json['policies']['placement']['requirements']:
                    data['requirements']['regions'] = job_json['policies']['placement']['requirements']['regions']
            if 'preferences' in job_json['policies']['placement']:
                if 'sites' in job_json['policies']['placement']['preferences']:
                    data['preferences']['sites'] = job_json['policies']['placement']['preferences']['sites']
                if 'regions' in job_json['policies']['placement']['preferences']:
                    data['preferences']['regions'] = job_json['policies']['placement']['preferences']['regions']

    return data

def scaler(db, shared_workers_deployed):
    """
    Scaler: create new workers as needed
    """
    logger.info('Started scaling cycle')

    shared_cpus = {}
    for job in jobs.get_idle_jobs():
        job_db = db.get_job(job['id'])

        if job['cpus'] > int(CONFIG.get('workers', 'shared_worker_cpu_threshold')):
            # Job should have a dedicated worker
            if not job_db:
                data = create_json(job)
                success = db.deployment_create(str(uuid.uuid4()), data, job['identity'], job['id'])

                if not job_db:
                    db.add_job(job['id'])
                else:
                    db.update_job(job['id'], 0)

        else:
            # Job should have a shared worker
            if job['group'] not in shared_cpus:
                shared_cpus['group'] = 0

            shared_cpus['group'] += job['cpus']

    if shared_workers_deployed[0] > 0:
        status = True
        infras = db.deployment_get_deployments_for_identity('shared')
        for infra in infras:
            if infra['status'] in ('creating', 'running'):
                status = False
        if status:
            shared_workers_deployed = (shared_workers_deployed[0], shared_workers_deployed[1], True)

    # Need to deploy new shared workers
    if shared_cpus:
        for group in shared_cpus:
            if shared_workers_deployed[0] == 0 or (shared_workers_deployed[0] > 0 and shared_workers_deployed[1]):
                workers_needed = int(math.ceil(float(shared_cpus[group])/float(CONFIG.get('workers', 'shared_worker_cpu_threshold'))))
                logger.info('Number of worker nodes needed for small jobs: %d', workers_needed)
                shared_workers_deployed = (workers_needed, shared_cpus, False)

                for counter in range(0, workers_needed):
                    data = create_json({'cpus': int(CONFIG.get('workers', 'shared_worker_cpus')),
                                        'memory': 1000*int(CONFIG.get('workers', 'shared_worker_memory')),
                                        'disk': 1000*1000*int(CONFIG.get('workers', 'shared_worker_disk')),
                                        'nodes': 1,
                                        'id': 0})
                    db.deployment_create(str(uuid.uuid4()), data, 'shared', 0)

    logger.info('Ended scaling cycle')
    return shared_workers_deployed

if __name__ == "__main__":
    shared_workers_deployed = (0, 0, False)
    while True:
        logger.info('Connecting to the DB')
        db = database.get_db()
        if db.connect():
            shared_workers_deployed = scaler(db, shared_workers_deployed)
            logger.info('Shared workers deployed=%d,%d,%d', shared_workers_deployed[0], shared_workers_deployed[1], shared_workers_deployed[2])
            db.close()
        else:
            logger.critical('Unable to connect to database')

        time.sleep(int(CONFIG.get('polling', 'scaler')))

