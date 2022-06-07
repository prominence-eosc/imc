import logging
import time

# Logging
logger = logging.getLogger(__name__)

def set_cloud_updated_quotas(self, cloud, identity):
    """
    Set time when quotas were updated
    """
    return self.execute("UPDATE status SET updated_quotas=%s WHERE identity='%s' AND name='%s'" % (time.time(), identity, cloud))

def get_cloud_updated_quotas(self, cloud, identity):
    """
    Get time when quotas were updated
    """
    updated = 0
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT updated_quotas FROM status WHERE identity='%s' AND name='%s'" % (identity, cloud))
        result = cursor.fetchone()
        if result:
            updated = result[0]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in get_cloud_updated_quotas due to: %s', err)

    return 0

def set_cloud_static_quotas(self, cloud, identity, limit_cpus, limit_memory, limit_instances):
    """
    Set static quotas
    """
    return self.execute("UPDATE status SET limit_cpus=%s,limit_memory=%s,limit_instances=%s WHERE identity='%s' AND name='%s'" % (limit_cpus, limit_memory, limit_instances, identity, cloud))

def set_cloud_dynamic_quotas(self, cloud, identity, remaining_cpus, remaining_memory, remaining_instances):
    """
    Set remaining resources
    """
    return self.execute("UPDATE status SET remaining_cpus=%s,remaining_memory=%s,remaining_instances=%s WHERE identity='%s' AND name='%s'" % (remaining_cpus, remaining_memory, remaining_instances, identity, cloud))
