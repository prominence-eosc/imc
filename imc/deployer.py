"""Wrapper for deploying infrastructure"""
from __future__ import print_function
import time
import random
import logging

from imc import config
from imc import database
from imc import provisioner
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger('imc.deployer')

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
            success = provisioner.deploy_job(db, infra_id)
        except Exception as exc:
            logger.info('Got exception deploying the job: %s', exc)
            success = False

        if success is None:
            logger.info('Setting status to unable due to a permanent failure')
            db.deployment_update_status(infra_id, 'unable')
        elif not success:
            logger.info('Setting status to waiting due to a temporary failure')
            db.deployment_update_status(infra_id, 'waiting')
        db.close()

    if not success:
        logger.critical('Unable to deploy infrastructure on any cloud')

    logger.info('Completed deploying infrastructure')
