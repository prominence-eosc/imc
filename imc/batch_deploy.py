"""Run pilot job on the specified batch system, with extensive error handling"""

from __future__ import print_function
import base64
import time
import logging

import radical.saga as rs

from imc import config
from imc import database
from imc import destroy
from imc import batchclient
from imc import opaclient
from imc import make_x509_proxy
from imc import tokens

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def create_job(unique_id, desc):
    """
    Create job description
    """
    # Create the proxy for HTCondor authentication
    expiry_time = int(time.time()) + 30*24*60*60
    proxy = make_x509_proxy.make_x509_proxy(CONFIG.get('credentials', 'host-cert'),
                                            CONFIG.get('credentials', 'host-key'),
                                            expiry_time,
                                            is_legacy_proxy=False,
                                            cn=None)

    # Required environment variables
    env = {}
    env['PROMINENCE_HOSTNAME'] = desc['job_want']
    env['PROMINENCE_WANT'] = desc['job_want']
    env['PROMINENCE_PROXY'] = (base64.b64encode(proxy)).decode("utf-8")

    # Job description
    jd = rs.job.Description()
    jd.wall_time_limit = desc['walltime']
    jd.processes_per_host = desc['cpus']
    jd.total_cpu_count = desc['cpus']*desc['nodes']
    jd.executable = desc['executable']
    jd.environment = env
    if 'queue' in desc:
        jd.queue = desc['queue']
    if 'project' in desc:
        jd.project = desc['project']
    jd.output = "%s/job-%s.out" % (desc['log_path'], unique_id)
    jd.error = "%s/job-%s.err" % (desc['log_path'], unique_id)
    return jd

def deploy(resource_name, time_begin, unique_id, identity, desc, db, batch_client):
    """
    Submit a startd job to the specified batch system
    """
    # Setup Open Policy Agent client
    opa_client = opaclient.OPAClient(url=CONFIG.get('opa', 'url'),
                                     timeout=int(CONFIG.get('opa', 'timeout')))

    retries_per_cloud = int(CONFIG.get('deployment', 'retries'))
    retry = 0
    success = False
    time_begin_this_cloud = time.time()

    batch_client.set(resource_name)

    # Retry loop
    while retry < retries_per_cloud + 1 and not success:
        if retry > 0:
            time.sleep(int(CONFIG.get('polling', 'duration')))
        logger.info('Deployment attempt %d of %d', retry+1, retries_per_cloud+1)
        retry += 1

        # Check if we should stop
        (_, infra_status_new, _, _, _) = db.deployment_get_im_infra_id(unique_id)
        if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
            logger.info('Deletion requested of infrastructure, aborting deployment')
            return None

        # Connect to batch system if necessary
        if not batch_client.exists(resource_name):
            batch_client.connect(resource_name)

        # Create infrastructure
        job = create_job(unique_id, desc)
        job_id = batch_client.create(job, resource_name)

        if job_id:
            logger.info('Submitted to batch system %s with job id %s and waiting for it to run', resource_name, job_id)
            db.deployment_update_status(unique_id, None, resource_name, job_id, 'batch')

            time_created = time.time()
            state_previous = None

            # Wait for infrastructure to enter the configured state
            while True:
                # Sleep
                time.sleep(int(CONFIG.get('polling', 'duration')))

                # Check if we should stop
                (_, infra_status_new, _, _, _) = db.deployment_get_im_infra_id(unique_id)
                if infra_status_new in ('deletion-requested', 'deleted', 'deletion-failed', 'deleting'):
                    logger.info('Deletion requested of infrastructure so aborting deployment')
                    return None

                # Don't spend too long trying to create infrastructure, give up eventually
                if time.time() - time_begin > int(CONFIG.get('timeouts', 'total')):
                    logger.info('Giving up, total time waiting is too long, so will destroy infrastructure with job id %s', job_id)
                    opa_client.set_status(resource_name, 'pending-too-long')
                    destroy.delete(unique_id, batch_client)
                    return None

                # Get the current overall state & states of all VMs in the infrastructure
                state = batch_client.getstate(job_id, resource_name)

                # If state is not known, wait
                if not state:
                    logger.info('State is not known for infrastructure with job id %s on batch system %s', job_id, resource_name)
                    continue

                # Log a change in state
                if state != state_previous:
                    logger.info('Infrastructure with job id %s is in state %s', job_id, state)
                    state_previous = state

                # Destroy infrastructure if it takes too long to start running
                if time.time() - time_created > int(CONFIG.get('timeouts', 'configured')):
                    logger.warning('Waiting too long for infrastructure to be completed, so destroying')
                    opa_client.set_status(resource_name, 'configuration-too-long')
                    destroy.delete(unique_id, batch_client)
                    return None

                # Handle different states
                if state == 'Running':
                    return job_id
                elif state in ('Failed', 'Canceled', 'Done'):
                    opa_client.set_status(resource_name, 'creation-failed')
                    return None
                elif state == 'Suspended':
                    logger.info('Deleting suspended job with job id %s', job_id)
                    destroy.delete(unique_id, batch_client)
                    return None
                
