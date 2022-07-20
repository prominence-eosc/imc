#!/usr/bin/env python
"""Destroys infrastructure"""

from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import RotatingFileHandler
import random
import signal
import sys
import time

from imc import config
from imc import database
from imc import utilities
from imc import destroyer

# Configuration
CONFIG = config.get_config()

# Logging
handler = RotatingFileHandler(filename=CONFIG.get('logs', 'filename').replace('imc.log', 'destroyer.log'),
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

def find_new_infra_for_deletion(db, executor):
    """
    Find infrastructure to be destroyed
    """
    infras = db.deployment_get_infra_in_state_cloud('deletion-requested')
    current_destroyers = 0
    num_not_run = 0

    if len(infras) > 0:
        logger.info('Found %d infrastructures to delete', len(infras))

    for infra in infras:
        if current_destroyers + 1 < int(CONFIG.get('pool', 'deleters')):
            logger.info('Running destroyer for infra %s', infra['id'])
            db.deployment_update_status(infra['id'], 'deleting')
            executor.submit(destroyer.destroyer, infra['id'])
            current_destroyers += 1
        else:
            num_not_run += 1

    if num_not_run > 0:
        logger.info('Not running %d destroyers as we already have enough', num_not_run)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_signal)

    executor = ThreadPoolExecutor(int(CONFIG.get('pool', 'deleters')))

    logger.info('Entering main polling loop')
    while True:
        if EXIT_NOW:
            logger.info('Exiting')
            sys.exit(0)
 
        db = database.get_db()
        if db.connect():
            find_new_infra_for_deletion(db, executor)
            db.close()
        else:
            logger.critical('Unable to connect to database')

        time.sleep(int(CONFIG.get('polling', 'destroyer')))

