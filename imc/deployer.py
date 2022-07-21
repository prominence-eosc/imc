"""Wrapper for deploying infrastructure"""
import time
import random
import logging
from logging.handlers import RotatingFileHandler

from imc import config
from imc import database
from imc import provisioner
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
handler = RotatingFileHandler(filename=CONFIG.get('logs', 'filename').replace('imc.log', 'provisioner.log'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(processName)s %(threadName)s %(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('imc')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def deployer(infra_id):
    """
    Deploy infrastructure
    """
    logger.info('Starting deployment of infrastructure %s', infra_id)

    # Random sleep
    time.sleep(random.randint(0, 4))

    db = database.get_db()
    if db.connect():
        logger.info('Connected to DB, about to deploy infrastructure for job')

        # Deploy infrastructure
        try:
            provisioner.provisioner(db, infra_id)
        except Exception as exc:
            logger.info('Got exception deploying the job: %s', exc)

        db.close()

    logger.info('Completed deploying infrastructure')
