import logging
import time

import radical.saga as rs

# Logging
logger = logging.getLogger(__name__)

class BatchClient(object):
    """
    Batch system helper
    """
    def __init__(self, config_list):
        self._job_services = {}
        self._last_used = {}
        self._config_list = config_list

    def set(self, name):
        self._last_used[name] = time.time()

    def cleanup(self):
        sites = self.list_resources()
        for site in sites:
            job_ids = self.list_job_ids(site)
            if len(job_ids) == 0 and time.time() - self._last_used[site] > 800:
                logger.info('Closing batch system %s', site)
                self.close(site)

    def close(self, name=None):
        for service in self._job_services:
            if not name or name == service:
                logger.info('Removing context for batch system %s', service)
                try:
                    self._job_services[service].close()
                except Exception as exc:
                    logger.error('Got exception when closing connection to %s: %s', service, exc)
                del self._job_services[service]

    def connect(self, name):
        for config in self._config_list:
            if config['type'] == 'batch':
                if name == config['name']:
                    logger.info('Adding context for batch system %s', name)
                    ctx = rs.Context("ssh")
                    ctx.user_id = config['credentials']['username']
                    ctx.user_key = config['credentials']['sshkey']
                    session = rs.Session()
                    session.add_context(ctx)
                    self._job_services[name] = rs.job.Service(config['credentials']['connectstring'], session=session)
                    

    def exists(self, name):
        if name in self._job_services:
            return True
        return False

    def list_resources(self):
        names = []
        for name in self._job_services:
            names.append(name)
        return names

    def list_job_ids(self, name):
        job_list = []
        try:
            for jid in self._job_services[name].list():
                job_list.append(jid)
        except Exception as exc:
            logger.error('Got exception listing jobs on %s: %s', name, exc)
        return job_list

    def create(self, job_description, name):
        self._last_used[name] = time.time()
        try:
            job = self._job_services[name].create_job(job_description)
            job.run()
        except Exception as exc:
            logger.error('Got exception running job on %s: %s', name, exc)
            return None
        return job.id

    def destroy(self, job_id, name):
        self._last_used[name] = time.time()
        try:
            job = self._job_services[name].get_job(job_id)
            job.cancel()
        except Exception as exc:
            logger.error('Got exception deleting job %s: %s', job_id, exc)

    def getstate(self, job_id, name):
        self._last_used[name] = time.time()
        try:
            job = self._job_services[name].get_job(job_id)
        except Exception as exc:
            logger.error('Got exception getting status of job %s: %s', job_id, exc)
            return None
        return job.state

