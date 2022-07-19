#!/usr/bin/env python
"""Worker lifecycle management"""

import logging
from logging.handlers import RotatingFileHandler
import time

from imc import config
from imc import database
from imc import workers

# Configuration
CONFIG = config.get_config()

# Logging
handler = RotatingFileHandler(filename=CONFIG.get('logs', 'filename').replace('.log', '-workers.log'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('imc')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def update_status(db, workers_list):
    """
    Update status of infrastructures
    """
    # Update workers to visible status
    infras = db.deployment_get_infra_in_state_cloud('running')
    for infra in infras:
        for worker in workers_list:
            if worker['id'] == infra['id']:
                logger.info('Changing status of infra %s to visible', infra['id'])
                db.deployment_update_status(infra['id'], 'visible')

    # Update workers to left status
    infras = db.deployment_get_infra_in_state_cloud('visible')
    for infra in infras:
        found = False
        for worker in workers_list:
            if worker['id'] == infra['id']:
                found = True
        if not found:
            logger.info('Changing status of infra %s to left', infra['id'])
            db.deployment_update_status(infra['id'], 'left')

    # Check for workers which have returned and workers which should be deleted
    infras = db.deployment_get_infra_in_state_cloud('left')
    for infra in infras:
        found = False
        for worker in workers_list:
            if worker['id'] == infra['id']:
                db.deployment_update_status(worker['id'], 'visible')
                found = True
        if not found and time.time() - infra['updated'] > int(CONFIG.get('workers', 'time_after_left')):
            logger.info('Worker with id %s left the pool, so deleting', infra['id'])
            db.deployment_update_status(infra['id'], 'deletion-requested')
        
    # Delete workers which never became visible
    infras = db.deployment_get_infra_in_state_cloud('running')
    for infra in infras:
        if time.time() - infra['updated'] > int(CONFIG.get('workers', 'max_time_since_creation')):
            found = False
            for worker in workers_list:
                if worker['id'] == infra['id']:
                    found = True
            if not found:
                logger.info('Worker with id %s never joined the pool, so deleting', infra['id'])
                db.deployment_update_status(infra['id'], 'deletion-requested')

if __name__ == "__main__":
    while True:
        logger.info('Connecting to the DB')
        db = database.get_db()
        if db.connect():
            # Get list of worker nodes
            logger.info('Getting list of workers')
            workers_list = workers.get_workers()

            logger.info('Updating status of workers')
            update_status(db, workers_list)

            db.close()
        else:
            logger.critical('Unable to connect to database')

        time.sleep(int(CONFIG.get('polling', 'workers')))
