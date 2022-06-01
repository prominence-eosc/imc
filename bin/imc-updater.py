#!/usr/bin/env python
"""Updates clouds available to each user"""

from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import RotatingFileHandler
import signal
import sys
import time

from imc import config
from imc import database
from imc import utilities
from imc import deployer
from imc import destroyer
from imc import cloud_updates
from imc import cloud_utils

# Configuration
CONFIG = config.get_config()

# Logging
handler = RotatingFileHandler(filename=CONFIG.get('logs', 'filename').replace('.log', '-updater.log'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(threadName)s %(name)s] %(message)s')
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

def updater(db, executors, last_fast_update_time):
    """
    Find identities which need checking
    """
    # Get list of all identities which have run jobs recently
    identities = db.deployment_get_identities()

    # Get list of infrastructures waiting to be deployed
    infras = db.deployment_get_infra_in_state_cloud('accepted', order=True)
    infras_waiting = db.deployment_get_infra_in_state_cloud('waiting', order=True)

    # Create full list of identities which potentially need their resources updated
    for infra in infras:
        if infra['identity'] not in identities:
            identities.append(infra['identity'])

    for infra in infras_waiting:
        if time.time() - infra['updated'] > int(CONFIG.get('updates', 'waiting')):
            infras.append(infra)

    # Check if we should run a fast check, per identity
    if time.time() - last_fast_update_time > 30*60:
        logger.info('Running fast checks...')
        last_fast_update_time = time.time()
        for identity in identities:
            logger.info('Submitting fast updater for identity %s', identity)
            executors.submit(cloud_updates.update, identity, False, False)
        # Static resources
        if len(identities) > 0:
            logger.info('Submitting fast updater for static resources')
            executors.submit(cloud_updates.update, 'static', False, True)

    # Check which identities need checks
    for identity in identities:
        (last_update_start, last_update) = db.get_resources_update(identity)
        if not last_update_start:
            logger.info('Submitting updater for identity %s as it has not run before', identity)
            executors.submit(cloud_updates.update, identity, True, False)
        elif time.time() - last_update > int(CONFIG.get('updates', 'discover')) and \
            time.time() - last_update_start > int(CONFIG.get('updates', 'deadline')):
            logger.info('Submitting updater for identity %s', identity)
            executors.submit(cloud_updates.update, identity, True, False)

    # Check static resources
    checked_static = False
    if len(identities) > 0:
        (last_update_start, last_update) = db.get_resources_update('static')
        if not last_update_start:
            logger.info('Submitting updater for static resources')
            executors.submit(cloud_updates.update, 'static', True, True)
            checked_static = True
        elif time.time() - last_update > int(CONFIG.get('updates', 'discover')) and \
            time.time() - last_update_start > int(CONFIG.get('updates', 'deadline')):
            logger.info('Submitting updater for static resources')
            executors.submit(cloud_updates.update, 'static', True, True)
            checked_static = True

    # Check for any new static or user-defined resources
    if cloud_utils.check_for_new_clouds(db, 'static') and not checked_static:
        logger.info('Running updates due to new clouds')
        executors.submit(cloud_updates.update, 'static', True, True)

    return last_fast_update_time

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info('Creating thread pool')
    executors = ThreadPoolExecutor(int(CONFIG.get('pool', 'updaters')))

    logger.info('Entering main polling loop')
    last_fast_update_time = 0

    while True:
        if EXIT_NOW:
            logger.info('Exiting')
            sys.exit(0)
 
        db = database.get_db()
        if db.connect():
            last_fast_update_time = updater(db, executors, last_fast_update_time)
            db.close()
        else:
            logger.critical('Unable to connect to database')

        time.sleep(int(CONFIG.get('polling', 'updater')))

