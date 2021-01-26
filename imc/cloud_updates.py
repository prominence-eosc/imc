from __future__ import print_function
import random
import time
import logging

from imc import config
from imc import egi_discover
from imc import utilities
from imc import cloud_images_flavours
from imc import cloud_functional_checks
from imc import cloud_quotas
from imc import cloud_utils
from imc import policies
from imc import tokens
from imc import database

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def update(identity):
    """
    Update cloud status, images, flavours
    """
    db = database.get_db()
    if not db.connect():
        logger.critical('Unable to connect to DB for updating identity %s', identity)
        return

    logger.info('Starting to update clouds for identity %s', identity)

    # Update the database
    db.set_resources_update_start(identity)

    # Spread out potentially concurrent updates slightly
    time.sleep(random.randint(1,5))

    # Update list of clouds if necessary
    egi_discover.egi_clouds_update(identity, db)

    # Get full list of cloud info
    clouds_info_list = cloud_utils.create_clouds_list(db, identity)

    # Check if clouds are functional
    logger.info('Checking if clouds are functional using their APIs')
    cloud_functional_checks.update_clouds_status(db, identity, clouds_info_list)

    # Update cloud images & flavours if necessary
    logger.info('Updating cloud images and flavours if necessary')
    try:
        cloud_images_flavours.update(db, identity, clouds_info_list)
    except Exception as err:
        logger.critical('Got exception in cloud_images_flavours: %s', err)

    # Update the database and close it
    db.set_resources_update(identity)
    db.close()

    logger.info('Finished updating clouds for identity %s', identity)
        
    return
