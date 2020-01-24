#!/usr/bin/env python
"""Deploy infrastructure"""

from __future__ import print_function
import logging
import random
import time

from imc import database
from imc import multicloud_deploy
from imc import imclient
from imc import opaclient
from imc import tokens
from imc import utilities

# Configuration
CONFIG = utilities.get_config()

def deployer(infra_id):
    """
    Deploy infrastructure
    """
    logging.basicConfig(filename=CONFIG.get('logs', 'filename').replace('.log', '-deployer-%s.log' % infra_id),
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')

    logging.info('Starting deployment of infrastructure')

    # Random sleep
    time.sleep(random.randint(0, 4))

    db = database.get_db()
    if db.connect():
        logging.info('Connected to DB, about to deploy infrastructure for job')

        # Deploy infrastructure
        success = multicloud_deploy.deploy_job(db, infra_id)

        if not success:
            db.deployment_update_status_with_retries(infra_id, 'unable')
        db.close()

    if not success:
        logging.critical('Unable to deploy infrastructure on any cloud')

    logging.info('Completed deploying infrastructure')
