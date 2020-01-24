#!/usr/bin/env python
"""Destroy infrastructure"""

from __future__ import print_function
import logging
import random
import time

from imc import database
from imc import destroy
from imc import imclient
from imc import opaclient
from imc import tokens
from imc import utilities

# Configuration
CONFIG = utilities.get_config()

def destroyer(infra_id):
    """
    Destroy infrastructure
    """
    logging.basicConfig(filename=CONFIG.get('logs', 'filename').replace('.log', '-destroyer-%s.log' % infra_id),
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')

    logging.info('Starting deletion of infrastructure')

    # Random sleep
    time.sleep(random.randint(0, 4))

    db = database.get_db()
    if db.connect():
        destroy.delete(infra_id)
        db.close()

    logging.info('Completed deleting infrastructure')
