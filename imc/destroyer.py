"""Destroy the specified IM infrastructure, with retries"""

from __future__ import print_function
import random
import time
import logging

from imc import config
from imc import database
from imc import destroy
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def destroyer(infra_id):
    """
    Destroy infrastructure
    """
    logging.basicConfig(filename=CONFIG.get('logs', 'filename').replace('.log', '-destroy-%s.log' % infra_id),
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')

    logging.info('Starting deletion of infrastructure %s', infra_id)

    # Random sleep
    time.sleep(random.randint(0, 4))

    db = database.get_db()
    if db.connect():
        destroy.delete(infra_id)
        db.close()

    logging.info('Completed deleting infrastructure')
