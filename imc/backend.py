""" Worker pool """

from __future__ import print_function
import configparser
import logging
from logging.handlers import RotatingFileHandler
import re
import os
import psutil
import sys
import subprocess
import time

import database
import imc
import imclient
import opaclient
import tokens
import utilities

# Configuration
CONFIG = configparser.ConfigParser()
if 'PROMINENCE_IMC_CONFIG_DIR' in os.environ:
    CONFIG.read('%s/imc.ini' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
else:
    print('ERROR: Environment variable PROMINENCE_IMC_CONFIG_DIR has not been defined')
    exit(1)

# Logging
handler = RotatingFileHandler(filename=CONFIG.get('logs', 'filename').replace('.log', '-backend.log'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('backend')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Initialize DB if necessary
dbi = database.get_db()
dbi.init()

def get_num_deployers():
    """
    Count the number of running deployment workers
    """
    count = 0
    for proc in psutil.process_iter():
        if len(proc.cmdline()) > 1:
            if proc.cmdline()[1] == '/imc/deployer.py':
                count += 1
    return count

def get_num_destroyers():
    """
    Count the number of running destroyer workers
    """
    count = 0
    for proc in psutil.process_iter():
        if len(proc.cmdline()) > 1:
            if proc.cmdline()[1] == '/imc/destroyer.py':
                count += 1
    return count

def find_new_infra_for_creation(db):
    """
    Find infrastructure to be deployed
    """
    infras = db.deployment_get_infra_in_state_cloud('accepted', None)
    current_deployers = get_num_deployers()
    num_not_run = 0

    for infra in infras:
        if current_deployers + 1 < int(CONFIG.get('pool', 'deployers')):
            logger.info('Running deploying for infra %s', infra['id'])
            subprocess.Popen(['python3', '/imc/deployer.py', infra['id']])
            current_deployers += 1
        else:
            num_not_run += 1

    if num_not_run > 0:
        logger.info('Not running %d deployers as we already have enough', num_not_run)

def find_new_infra_for_deletion(db):
    """
    Find infrastructure to be destroyed
    """
    infras = db.deployment_get_infra_in_state_cloud('deletion-requested', None)
    current_destroyers = get_num_destroyers()
    num_not_run = 0

    for infra in infras:
        if current_destroyers + 1 < int(CONFIG.get('pool', 'deleters')):
            logger.info('Running destroyer for infra %s', infra['id'])
            subprocess.Popen(['python3', '/imc/destroyer.py', infra['id']])
            current_destroyers += 1
        else:
            num_not_run += 1

    if num_not_run > 0:
        logger.info('Not running %d destroyers as we already have enough', num_not_run)

if __name__ == "__main__":
    while True:
        db = database.get_db()
        if db.connect():
            logger.info('Checking for new infrastructures to deploy...')
            find_new_infra_for_creation(db)
            logger.info('Checking for new infrastructures to delete...')
            find_new_infra_for_deletion(db)
            db.close()
        time.sleep(30)

