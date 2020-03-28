"""Deploy infrastructure on the specified cloud, with extensive error handling"""

from __future__ import print_function
import time
import random
import logging

from imc import config
from imc import database
from imc import multicloud_deploy
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def deployer(infra_id):
    """
    Deploy infrastructure
    """
    logging.basicConfig(filename=CONFIG.get('logs', 'filename').replace('.log', '-deploy-%s.log' % infra_id),
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

