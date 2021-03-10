import logging
import time

# Logging
logger = logging.getLogger(__name__)

def get_cloud_info(self, cloud, identity):
    """
    Get cloud status and quotas
    """
    (status, mon_status, limit_cpus, limit_memory, limit_instances, remaining_cpus, remaining_memory, remaining_instances) = (-1, -1, -1, -1, -1, -1, -1, -1)
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT status, mon_status, limit_cpus, limit_memory, limit_instances, remaining_cpus, remaining_memory, remaining_instances FROM clouds_info WHERE name='%s' AND identity='%s'" % (cloud, identity))
        for row in cursor:
            status = row[0]
            mon_status = row[1]
            limit_cpus = row[2]
            limit_memory = row[3]
            limit_instances = row[4]
            remaining_cpus = row[5]
            remaining_memory = row[6]
            remaining_instances = row[7]
        cursor.close()
    except Exception as error:
        logger.critical('[get_cloud_info] Unable to execute SELECT query due to: %s', error)

    return (status, mon_status, limit_cpus, limit_memory, limit_instances, remaining_cpus, remaining_memory, remaining_instances)

def set_cloud_updated_quotas(self, cloud, identity):
    """
    Set time that quotas where updated
    """
    return self.execute("UPDATE clouds_info SET updated_quotas=%s WHERE identity='%s' AND name='%s'" % (time.time(), identity, cloud))

def set_cloud_mon_status(self, cloud, identity, status):
    """
    Set time when monitoring info was updated
    """
    return self.execute("UPDATE clouds_info SET mon_status=%s WHERE identity='%s' AND name='%s'" % (status, identity, cloud))

def set_cloud_status(self, cloud, identity, status):
    """
    Set time when cloud status was updated
    """
    return self.execute("UPDATE clouds_info SET status=%s WHERE identity='%s' AND name='%s'" % (status, identity, cloud))

def init_cloud_info(self, cloud, identity):
    """
    Initialise a cloud name and user
    """
    return self.execute("INSERT INTO clouds_info (name, identity) SELECT '%s', '%s' WHERE NOT EXISTS (SELECT 1 FROM clouds_info WHERE name='%s' AND identity='%s')" % (cloud, identity, cloud, identity))

def get_deployment_failures(self, identity, interval, successes=False):
    """
    Get list of deployment failures
    """
    where = ''
    if successes:
        where = 'AND reason=0'

    output = {}
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT COUNT(*), cloud FROM deployment_failures WHERE identity='%s' %s AND time > %s GROUP BY cloud" % (identity, where, time.time() - interval))
        for row in cursor:
            output[row[1]] = row[0]
        cursor.close()
    except Exception as error:
        logger.critical('[get_deployment_failures] Unable to execute SELECT query due to: %s', error)
        return output

    return output

def del_old_deployment_failures(self, interval):
    """
    Delete old deployment failures
    """
    return self.execute("DELETE FROM deployment_failures WHERE time < %s" % (time.time() - interval))

def set_resources_update(self, identity):
    """
    Update time when clouds were updated
    """
    return self.execute("INSERT INTO cloud_updates (identity, time) VALUES (%s, %s) ON CONFLICT (identity) DO UPDATE SET time=EXCLUDED.time", (identity, time.time()))

def set_resources_update_start(self, identity):
    """
    Update time when clouds updated began
    """
    return self.execute("INSERT INTO cloud_updates (identity, start) VALUES (%s, %s) ON CONFLICT (identity) DO UPDATE SET start=EXCLUDED.start", (identity, time.time()))

def get_resources_update(self, identity):
    """
    Get time when clouds were updated for the specified user
    """
    update_start = 0
    updated = 0

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT start, time FROM cloud_updates WHERE identity='%s'" % identity)
        for row in cursor:
            update_start = row[0]
            updated = row[1]
        cursor.close()
    except Exception as error:
        logger.critical('[get_resources_update] Unable to get update time due to %s', error)

    return (int(update_start), int(updated))
