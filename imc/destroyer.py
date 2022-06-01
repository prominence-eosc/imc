"""Destroy the specified infrastructure, with retries"""

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
logger = logging.getLogger('imc.destroyer')

def destroyer(infra_id):
    """
    Destroy infrastructure
    """
    logger.info('Starting deletion of infrastructure %s', infra_id)

    # Random sleep
    time.sleep(random.randint(0, 4))

    db = database.get_db()
    if db.connect():
        try:
            destroy.delete(infra_id)
        except Exception as exc:
            logging.info('Got exception running delete: %s', exc)
        db.close()

    logger.info('Completed deleting infrastructure')
