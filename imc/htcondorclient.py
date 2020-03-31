import classad
import htcondor

class HTCondorClient(object):
    """
    HTCondor helper
    """
    def __init__(self):
        self._schedd = htcondor.Schedd()
        self._required_attrs = ['ClusterId', 'JobStatus']

    def list_job_ids(self):
        """
        List infrastructure IDs
        """
        jobs_output = []
        jobs = schedd.xquery('True', self._required_attrs)
        for job in jobs:
            if 'ClusterId' in job:
                jobs_output.append(job['ClusterId'])
        return jobs_output

    def getstate(self, job_id):
        """
        Get overall job status
        """
        jobs = self._schedd.xquery('ClusterId =?= %d' % int(job_id), self._required_attrs)
        for job in jobs:
            if 'ClusterId' in job and 'JobStatus' in job:
                return int(job['JobStatus'])

        # Job must have finished
        jobs = self._schedd.history('ClusterId =?= %d' % int(job_id), self._required_attrs)
        for job in jobs:
            if 'ClusterId' in job and 'JobStatus' in job:
                return int(job['JobStatus'])

        # Job doesn't exist
        return None

    def destroy(self, job_id):
        """
        Destroy job
        """
        ret = self._schedd.act(htcondor.JobAction.Remove, 'ClusterId =?= %d' % int(job_id))

        if ret["TotalSuccess"] > 0:
            return 0
        return 1

    def create(self, htcondor_job):
        """
        Submit a job & return the job id
        """
        id = None
        try:
            sub = htcondor.Submit(htcondor_job)
            with self._schedd.transaction() as txn:
                id = sub.queue(txn, 1)
        except Exception as err:
            return None

        if id:
            return id
        return None

