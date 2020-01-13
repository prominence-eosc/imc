""" Worker pool """

from __future__ import print_function
import configparser
import logging
import re
import os
import sys
import time

import database
import imc
import imclient
import opaclient
import tokens
import utilities
import logger as custom_logger

# Configuration
CONFIG = configparser.ConfigParser()
if 'PROMINENCE_IMC_CONFIG_DIR' in os.environ:
    CONFIG.read('%s/imc.ini' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
else:
    print('ERROR: Environment variable PROMINENCE_IMC_CONFIG_DIR has not been defined')
    exit(1)

if __name__ == "__main__":
    infra_id = sys.argv[1]
    logging.basicConfig(filename=CONFIG.get('logs', 'filename').replace('.log', '-deployer-%s.log' % infra_id),
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')
    
    logging.info('Starting deployment of infrastructure')

    db = database.get_db()
    if db.connect():
        logging.info('Connected to DB, about to deploy infrastructure for job')

        # Get JSON description & identity from the DB
        (description, identity) = db.deployment_get_json(infra_id)

         # Get RADL
        radl_contents = utilities.get_radl(description)
        if not radl_contents:
            logging.critical('RADL must be provided')
            db.deployment_update_status_with_retries(infra_id, 'unable')
            db.close()
            exit(1)

        # Get requirements & preferences
        (requirements, preferences) = utilities.get_reqs_and_prefs(description)

        # Deploy infrastructure
        success = imc.deploy_job(db, radl_contents, requirements, preferences, infra_id, identity, False)

        if not success:
            db.deployment_update_status_with_retries(infra_id, 'unable')
        db.close()

    if not success:
        logging.critical('Unable to deploy infrastructure on any cloud')

    logging.info('Completed deploying infrastructure')

