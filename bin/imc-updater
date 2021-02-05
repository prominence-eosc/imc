#!/usr/bin/env python
"""Updates clouds available to each user"""

from __future__ import print_function
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

    # Create full list of identities which potentially need their resources updated
    for infra in infras:
        if infra['identity'] not in identities:
            identities.append(infra['identity'])

    # Check which identities need checks
    for identity in identities:
        (last_update_start, last_update) = db.get_resources_update(identity)
        if time.time() - last_update > int(CONFIG.get('updates', 'discover')) and \
           time.time() - last_update_start > int(CONFIG.get('updates', 'deadline')):
            logger.info('Submitting updater for identity %s', identity)
            executors.submit(cloud_updates.update, identity)
        elif time.time() - last_fast_update_time > 30*60:
            last_fast_update_time = time.time()
            logger.info('Submitting fast updater for identity %s', identity)
            executors.submit(cloud_updates.update, identity, 1)

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

        time.sleep(30)

