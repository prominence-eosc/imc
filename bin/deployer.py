#!/usr/bin/env python
"""Deploy infrastructure"""

from concurrent.futures import ProcessPoolExecutor
import logging
from logging.handlers import RotatingFileHandler
import random
import signal
import sys
import time

from imc import config
from imc import database
from imc import utilities
from imc import deployer

# Configuration
CONFIG = config.get_config()

# Logging
handler = RotatingFileHandler(filename=CONFIG.get('logs', 'filename').replace('imc.log', 'deployer.log'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(processName)s %(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('imc')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Initialize DB if necessary
dbi = database.get_db()
dbi.init()

EXIT_NOW = False

def handle_signal(signum, frame):
    """
    Handle signals
    """
    global EXIT_NOW
    EXIT_NOW = True
    logger.info('Received signal %d, shutting down...', signum)

def find_new_infra_for_creation(db, executor):
    """
    Find infrastructure to be deployed
    """
    # Get all new infrastructures to deploy
    infras = db.deployment_get_infra_in_state_cloud('accepted', order=True)

    # Include any infrastructures which have been in the waiting state for long enough, with some
    # slight randomness added to ensure we don't get many starting at exactly the same time
    infras_waiting = db.deployment_get_infra_in_state_cloud('waiting', order=True)

    for infra in infras_waiting:
        if time.time() - infra['updated'] > int(CONFIG.get('updates', 'waiting')) + random.randint(-200, 200):
            infras.append(infra)
            
    current_deployers = 0
    num_not_run = 0

    if len(infras) > 0:
        logger.info('Found %d infrastructures to deploy', len(infras))

    for infra in infras:
        if current_deployers + 1 < int(CONFIG.get('pool', 'deployers')):
            logger.info('Running deploying for infra %s', infra['id'])
            db.deployment_update_status(infra['id'], 'creating')
            executor.submit(deployer.deployer, infra['id'])
            current_deployers += 1
        else:
            num_not_run += 1

    if num_not_run > 0:
        logger.info('Not running %d deployers as we already have enough', num_not_run)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info('Creating process pool')
    with ProcessPoolExecutor(int(CONFIG.get('pool', 'deployers'))) as executor:
        logger.info('Entering main polling loop')
        while True:
            if EXIT_NOW:
                logger.info('Exiting')
                sys.exit(0)
 
            db = database.get_db()
            if db.connect():
                find_new_infra_for_creation(db, executor)
                db.close()
            else:
                logger.critical('Unable to connect to database')

            time.sleep(int(CONFIG.get('polling', 'deployer')))
