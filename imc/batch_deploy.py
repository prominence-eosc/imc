"""Run pilot job on the specified batch system, with extensive error handling"""

from __future__ import print_function
import os
import sys
from string import Template
import time
import random
import logging

from imc import config
from imc import database
#from imc import htcondorclient
from imc import opaclient
from imc import tokens

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def create_job():
    """
    Create HTCondor pilot job
    """
    cjob = {}
    cjob['universe'] = 'grid'
    cjob['transfer_executable'] = 'true'
    cjob['executable'] = ''
    cjob['arguments'] = ''
    cjob['Log'] = ''
    cjob['Output'] = '' 
    cjob['Error'] = ''
    cjob['should_transfer_files'] = 'YES'
    cjob['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
    cjob['skip_filechecks'] = 'true'
    cjob['transfer_output_files'] = ''
    cjob['transfer_input_files'] = ''

    return cjob

def deploy(resource_name, time_begin, unique_id, identity, db, num_nodes=1):
    """
    Submit a startd job to the specified batch system
    """
    #client = htcondorclient.HTCondorClient()

    retries_per_cloud = int(CONFIG.get('deployment', 'retries'))
    retry = 0
    success = False
    time_begin_this_cloud = time.time()

    # Retry loop
    while retry < retries_per_cloud + 1 and not success:
        if retry > 0:
            time.sleep(int(CONFIG.get('polling', 'duration')))
        logger.info('Deployment attempt %d of %d', retry+1, retries_per_cloud+1)
        retry += 1

        # Check if we should stop
        (im_infra_id_new, infra_status_new, cloud_new, _, _) = db.deployment_get_im_infra_id(unique_id)
        if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
            logger.info('Deletion requested of infrastructure, aborting deployment')
            return None

        # Create infrastructure
        job = create_job()
        #job_id = client.create(job)

        if job_id:
            logger.info('Submitted to batch system %s with job id %d and waiting for it to run', resource_name, job_id)
            db.deployment_update_status_with_retries(unique_id, None, resource_name, str(job_id), 'batch')

