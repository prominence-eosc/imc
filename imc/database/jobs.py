import logging
import time

# Logging
logger = logging.getLogger(__name__)

def add_job(self, job_id):
    """
    Add a job
    """
    return self.execute("INSERT INTO jobs (id, creation, updated, status) VALUES (%s, %s, %s, %s)",
                        (job_id, time.time(), time.time(), 0))

def update_job(self, job_id, status):
    """
    Update job status
    """
    return self.execute("UPDATE jobs SET status=%s WHERE id=%s" % (status, job_id))

def get_job(self, job_id):
    """
    Get job
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT creation, updated, status FROM jobs WHERE id=%d" % job_id)
        result = cursor.fetchone()
        if result:
            return result[0], result[1], result[2]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in get_job due to: %s', err)

    return None
