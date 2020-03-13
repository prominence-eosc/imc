"""Destroy the specified IM infrastructure, with retries"""

from __future__ import print_function
import random
import time
import logging

from imc import database
from imc import cloud_destroy
from imc import utilities

# Configuration
CONFIG = utilities.get_config()

# Logging
logger = logging.getLogger(__name__)

def destroyer(infra_id):
    """
    Destroy infrastructure
    """
    logging.basicConfig(filename=CONFIG.get('logs', 'filename').replace('.log', '-destroy-%s.log' % infra_id),
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')

    logging.info('Starting deletion of infrastructure')

    # Random sleep
    time.sleep(random.randint(0, 4))

    db = database.get_db()
    if db.connect():
        cloud_destroy.delete(infra_id)
        db.close()

    logging.info('Completed deleting infrastructure')
