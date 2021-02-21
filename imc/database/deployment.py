import logging
import time
from psycopg2.extras import Json

# Logging
logger = logging.getLogger(__name__)

def deployment_get_infra_in_state_cloud(self, state, cloud=None, order=False):
    """
    Return a list of all infrastructure IDs for infrastructure in the specified state and cloud
    """
    query = ""
    if cloud:
        query = "AND cloud='%s'" % cloud
    if order:
        query += " ORDER BY creation ASC"
    infra = []
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT id,creation,updated,identity FROM deployments WHERE status='%s' %s" % (state, query))
        for row in cursor:
            infra.append({"id":row[0], "created":row[1], "updated":row[2], "identity":row[3]})
        cursor.close()
    except Exception as error:
        logger.critical('[deployment_get_infra_in_state_cloud] Unable to execute query due to: %s', error)
    return infra

def deployment_check_infra_id(self, infra_id):
    """
    Ceck if the given infrastructure ID exists
    """
    number = 0

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT count(*) FROM deployments WHERE id='%s'" % infra_id)
        for row in cursor:
            number = row[0]
        cursor.close()
    except Exception as error:
        logger.critical('[deployment_check_infra_id] Unable to execute query due to: %s', error)
        return 2

    if number > 0:
        return 0
    return 1

def deployment_get_resource_type(self, infra_id):
    """
    Return the resource type for the current status
    """
    resource_type = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT resource_type FROM deployments WHERE id='%s'" % infra_id)
        for row in cursor:
            resource_type = row[0]
        cursor.close()
    except Exception as error:
        logger.critical('[deployment_get_resource_type] Unable to execute query due to: %s', error)
    return resource_type

def deployment_get_status_reason(self, infra_id):
    """
    Return reason for the current status
    """
    status_reason = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT status_reason FROM deployments WHERE id='%s'" % infra_id)
        for row in cursor:
            status_reason = row[0]
        cursor.close()
    except Exception as error:
        logger.critical('[deployment_get_status_reason] Unable to execute query due to: %s', error)
    return status_reason

def deployment_get_identity(self, infra_id):
    """
    Return the identity associated with the infrastructure
    """
    identity = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT identity FROM deployments WHERE id='%s'" % infra_id)
        for row in cursor:
            identity = row[0]
        cursor.close()
    except Exception as error:
        logger.critical('[deployment_get_identity] Unable to execute query due to: %s', error)
    return identity

def deployment_get_identities(self):
    """
    Get list of recent identities
    """
    identities = []

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT DISTINCT identity FROM deployments WHERE creation > %s" % int(time.time() - 48*60*60))
        for row in cursor:
            identities.append(row[0])
        cursor.close()
    except Exception as error:
        logger.critical('[deployment_get_identities] Unable to execute query due to: %s', error)

    return identities

def deployment_get_json(self, infra_id):
    """
    Return the json description associated with the infrastructure
    """
    description = None
    identity = None
    identifier = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT description,identity,identifier FROM deployments WHERE id='%s'" % infra_id)
        for row in cursor:
            description = row[0]
            identity = row[1]
            identifier = row[2]
        cursor.close()
    except Exception as error:
        logger.critical('[deployment_get_json] Unable to execute query due to: %s', error)
        return None, None

    return (description, identity, identifier)

def get_infra_from_im_infra_id(self, im_infra_id):
    """
    Check if the provided IM infra ID corresponds to known infrastructure
    """
    infra_id = None
    status = None
    cloud = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT id,status,cloud FROM deployments WHERE im_infra_id='%s'" % im_infra_id)
        for row in cursor:
            infra_id = row[0]
            status = row[1]
            cloud = row[2]
        cursor.close()
    except Exception as error:
        logger.critical('[deployment_infra_from_im_infra_id] Unable to execute query due to: %s', error)
    return (infra_id, status, cloud)

def deployment_get_im_infra_id(self, infra_id):
    """
    Return the IM infrastructure ID, our status and cloud name
    """
    im_infra_id = None
    status = None
    cloud = None
    created = None
    updated = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT im_infra_id,status,cloud,creation,updated FROM deployments WHERE id='%s'" % infra_id)
        for row in cursor:
            im_infra_id = row[0]
            status = row[1]
            cloud = row[2]
            created = row[3]
            updated = row[4]
        cursor.close()
    except Exception as error:
        logger.critical('[deployment_get_im_infra_id] Unable to execute query due to: %s', error)
    return (im_infra_id, status, cloud, created, updated)

def deployment_create_with_retries(self, infra_id, description, identity, identifier):
    """
    Create deployment with retries & backoff
    """
    max_retries = 10
    count = 0
    success = False
    while count < max_retries and not success:
        success = self.deployment_create(infra_id, description, identity, identifier)
        if not success:
            count += 1
            self.close()
            time.sleep(count/2)
            self.connect()
    return success

def create_im_deployment(self, infra_id, im_infra_id):
    """
    Log IM deployment
    """
    return self.execute("INSERT INTO deployment_log (id, im_infra_id, created) VALUES (%s,%s,%s)", (infra_id, im_infra_id, time.time()))

def delete_im_deployments(self, infra_id=None, since=None):
    """
    Delete old IM deployments
    """
    if infra_id and not since:
        return self.execute("DELETE FROM deployment_log WHERE id='%s'" % infra_id)
    elif infra_id and since:
        return self.execute("DELETE FROM deployment_log WHERE id='%s' and created<%s" % (infra_id, time.time()-since))
    elif since:
        return self.execute("DELETE FROM deployment_log WHERE created<%s" % time.time()-since)
    return None

def get_im_deployments(self, infra_id):
    """
    Get all IM infrastructure ids associated with an infrastructure
    """
    infra = []
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT im_infra_id FROM deployment_log WHERE id='%s'" % infra_id)
        for row in cursor:
            infra.append(row[0])
        cursor.close()
    except Exception as error:
        logger.critical('[get_im_deployments] Unable to execute query due to: %s', error)
    return infra

def check_im_deployment(self, im_infra_id):
    """
    Try to find the infra id associated with the given IM infrastructure id
    """
    infra = None
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT id FROM deployment_log WHERE im_infra_id='%s'" % im_infra_id)
        for row in cursor:
            infra = row[0]
        cursor.close()
    except Exception as error:
        logger.critical('[check_im_deployment] Unable to execute query due to: %s', error)

    return infra

def deployment_create(self, infra_id, description, identity, identifier):
    """
    Create deployment
    """
    return self.execute("INSERT INTO deployments (id,description,status,identity,identifier,creation,updated) VALUES (%s,%s,'accepted',%s,%s,%s,%s)", (infra_id, Json(description), identity, identifier, time.time(), time.time()))

def deployment_remove(self, infra_id):
    """
    Remove an infrastructure from the DB
    """
    return self.execute("DELETE FROM deployments WHERE id='%s'" % infra_id)

def deployment_log_remove(self, infra_id):
    """
    Remove an infrastructure from the DB
    """
    return self.execute("DELETE FROM deployment_log WHERE id='%s'" % infra_id)

def deployment_update_status(self, infra_id, status=None, cloud=None, im_infra_id=None, resource_type='cloud'):
    """
    Update deployment status
    """
    if cloud and im_infra_id and status:
        return self.execute("UPDATE deployments SET resource_type='%s',status='%s',cloud='%s',im_infra_id='%s',updated=%d WHERE id='%s'" % (resource_type, status, cloud, im_infra_id, time.time(), infra_id))
    elif cloud and status:
        return self.execute("UPDATE deployments SET resource_type='%s',status='%s',cloud='%s',updated=%d WHERE id='%s'" % (resource_type, status, cloud, time.time(), infra_id))
    elif im_infra_id and cloud and not status:
        return self.execute("UPDATE deployments SET resource_type='%s',cloud='%s',im_infra_id='%s',updated=%d WHERE id='%s'" % (resource_type, cloud, im_infra_id, time.time(), infra_id))
    elif status:
        if status in ('configured', 'waiting', 'unable', 'creating'):
            return self.execute("UPDATE deployments SET resource_type='%s',status='%s',updated=%d WHERE id='%s' AND status NOT IN ('deleted', 'deleting', 'deletion-requested', 'deletion-failed')" % (resource_type, status, time.time(), infra_id))
        else:
            return self.execute("UPDATE deployments SET resource_type='%s',status='%s',updated=%d WHERE id='%s'" % (resource_type, status, time.time(), infra_id))
    return False

def deployment_update_status_reason(self, infra_id, status_reason):
    """
    Update deploymeny status reason
    """
    return self.execute("UPDATE deployments SET status_reason='%s' WHERE id='%s'" % (status_reason, infra_id))

def deployment_update_resources(self, infra_id, used_instances, used_cpus, used_memory):
    """
    Update resources used by infra
    """
    return self.execute("UPDATE deployments SET used_instances=%s, used_cpus=%s, used_memory=%s WHERE id='%s'" % (used_instances, used_cpus, used_memory, infra_id))

def get_used_resources(self, identity, cloud):
    """
    Get the total resources used on a particular cloud
    """
    used_instances = 0
    used_cpus = 0
    used_memory = 0

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT SUM(used_instances), SUM(used_cpus), SUM(used_memory) FROM deployments WHERE status IN ('configured', 'creating') AND identity='%s' AND cloud='%s'" % (identity, cloud))
        for row in cursor:
            if row[0] and row[1] and row[2]:
                used_instances = int(row[0])
                used_cpus = int(row[1])
                used_memory = int(row[2])
        cursor.close()
    except Exception as error:
        logger.critical('[get_used_resources] Unable to execute query due to: %s', error)
    return (used_instances, used_cpus, used_memory)

def set_deployment_failure(self, cloud, identity, reason, duration=-1):
    """
    Set deployment failure reason
    """
    return self.execute("INSERT INTO deployment_failures (cloud, identity, reason, time, duration) VALUES (%s,%s,%s,%s,%s)", (cloud, identity, reason, time.time(), duration))
