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
    except Exception as err:
        logger.critical('Unable to execute query in deployment_get_infra_in_state_cloud due to: %s', err)
    return infra

def deployment_check_infra_id(self, infra_id):
    """
    Ceck if the given infrastructure ID exists
    """
    number = 0
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT count(*) FROM deployments WHERE id='%s'" % infra_id)
        result = cursor.fetchone()
        if result:
            number = result[0]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in deployment_check_infra_id due to: %s', err)
        return None

    if number > 0:
        return False
    return True

def deployment_get_status_reason(self, infra_id):
    """
    Return reason for the current status
    """
    status_reason = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT status_reason FROM deployments WHERE id='%s'" % infra_id)
        result = cursor.fetchone()
        if result:
            status_reason = result[0]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in deployment_get_status_reason due to: %s', err)
    return status_reason

def deployment_get_identity(self, infra_id):
    """
    Return the identity associated with the infrastructure
    """
    identity = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT identity FROM deployments WHERE id='%s'" % infra_id)
        result = cursor.fetchone()
        if result:
            identity = result[0]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in deployment_get_identity due to: %s', err)
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
    except Exception as err:
        logger.critical('Unable to execute query in deployment_get_identities due to: %s', err)

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
        result = cursor.fetchone()
        if result:
            description = result[0]
            identity = result[1]
            identifier = result[2]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in deployment_get_json due to: %s', err)
        return None, None

    return (description, identity, identifier)

def get_infra_from_infra_id(self, cloud_infra_id):
    """
    Check if the provided infra ID corresponds to known infrastructure
    """
    infra_id = None
    status = None
    cloud = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT id,status,cloud FROM deployments WHERE cloud_infra_id='%s'" % cloud_infra_id)
        result = cursor.fetchone()
        if result:
            infra_id = result[0]
            status = result[1]
            cloud = result[2]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in get_infra_from_infra_id due to: %s', err)
    return (infra_id, status, cloud)

def deployment_get_infra_id(self, infra_id):
    """
    Return the infrastructure ID, our status and cloud name
    """
    cloud_infra_id = None
    status = None
    cloud = None
    created = None
    updated = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT cloud_infra_id,status,cloud,creation,updated FROM deployments WHERE id='%s'" % infra_id)
        result = cursor.fetchone()
        if result:
            cloud_infra_id = result[0]
            status = result[1]
            cloud = result[2]
            created = result[3]
            updated = result[4]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in deployment_get_infra_id due to: %s', err)
    return (cloud_infra_id, status, cloud, created, updated)

def create_cloud_deployment(self, infra_id, unique_infra_id, cloud, identity):
    """
    Log deployment
    """
    return self.execute("INSERT INTO deployment_log (id, unique_infra_id, cloud, identity, created) VALUES (%s,%s,%s,%s,%s)",
                        (infra_id, unique_infra_id, cloud, identity, time.time()))

def update_cloud_deployment(self, unique_infra_id, cloud_infra_id):
    """
    Add cloud infrastructure id to deployment
    """
    return self.execute("UPDATE deployment_log SET cloud_infra_id='%s' WHERE unique_infra_id='%s'" % (cloud_infra_id, unique_infra_id))

def delete_deployments(self, infra_id=None, since=None):
    """
    Delete old deployments
    """
    if infra_id and not since:
        return self.execute("DELETE FROM deployment_log WHERE id='%s'" % infra_id)
    elif infra_id and since:
        return self.execute("DELETE FROM deployment_log WHERE id='%s' and created<%s" % (infra_id, time.time()-since))
    elif since:
        return self.execute("DELETE FROM deployment_log WHERE created<%s" % time.time()-since)
    return None

def get_deployments(self, infra_id):
    """
    Get all infrastructure ids associated with an infrastructure
    """
    infra = []
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT cloud_infra_id,cloud FROM deployment_log WHERE id='%s'" % infra_id)
        for row in cursor:
            infra.append({'id': row[0], 'cloud': row[1]})
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in get_deployments due to: %s', err)
    return infra

def get_deployment(self, infra_id):
    """
    Try to find the infra id associated with the given infrastructure id
    """
    infra = None
    unique_id = None
    cloud = None
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT id,unique_infra_id,cloud FROM deployment_log WHERE cloud_infra_id='%s'" % infra_id)
        result = cursor.fetchone()
        if result:
            infra = result[0]
            unique_id = result[1]
            cloud = result[2]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in get_deployment due to: %s', err)

    return infra, unique_id, cloud

def deployment_get_deployments_for_identity(self, identity):
    """
    """
    infras = []

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT id,creation,updated,status FROM deployments WHERE identity='%s'" % identity)
        for row in cursor:
            infras.append({'id': row[0], 'creation': row[1], 'updated': row[2], 'status': row[3]})
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in deployment_get_deployments_for_identity due to: %s', err)

    return infras

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

def deployment_update_status(self, infra_id, status=None, cloud=None, cloud_infra_id=None, resource_type='cloud'):
    """
    Update deployment status
    """
    if cloud and infra_id and status:
        return self.execute("UPDATE deployments SET resource_type='%s',status='%s',cloud='%s',cloud_infra_id='%s',updated=%d WHERE id='%s'" % (resource_type, status, cloud, cloud_infra_id, time.time(), infra_id))
    elif cloud and status:
        return self.execute("UPDATE deployments SET resource_type='%s',status='%s',cloud='%s',updated=%d WHERE id='%s'" % (resource_type, status, cloud, time.time(), infra_id))
    elif infra_id and cloud and not status:
        return self.execute("UPDATE deployments SET resource_type='%s',cloud='%s',cloud_infra_id='%s',updated=%d WHERE id='%s'" % (resource_type, cloud, cloud_infra_id, time.time(), infra_id))
    elif status:
        if status in ('left', 'visible', 'running', 'waiting', 'unable', 'creating'):
            return self.execute("UPDATE deployments SET resource_type='%s',status='%s',updated=%d WHERE id='%s' AND status NOT IN ('deleted', 'deleting', 'deletion-requested', 'deletion-failed')" % (resource_type, status, time.time(), infra_id))
        else:
            return self.execute("UPDATE deployments SET resource_type='%s',status='%s',updated=%d WHERE id='%s'" % (resource_type, status, time.time(), infra_id))
    return False

def deployment_update_status_reason(self, infra_id, status_reason):
    """
    Update deployment status reason
    """
    return self.execute("UPDATE deployments SET status_reason='%s' WHERE id='%s'" % (status_reason, infra_id))

def deployment_update_resources(self, infra_id, used_instances, used_cpus, used_memory):
    """
    Update resources used by infra
    """
    return self.execute("UPDATE deployments SET used_instances=%s, used_cpus=%s, used_memory=%s WHERE id='%s'" % (used_instances, used_cpus, used_memory, infra_id))

def get_used_resources(self, identity, cloud, creating=None):
    """
    Get the total resources used on a particular cloud
    """
    used_instances = 0
    used_cpus = 0
    used_memory = 0

    if creating:
        states = "'left', 'visible', 'running', 'creating'"
    else:
        states = "'left', 'visible', 'running'"

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT SUM(used_instances), SUM(used_cpus), SUM(used_memory) FROM deployments WHERE status IN (%s) AND identity='%s' AND cloud='%s'" % (states, identity, cloud))
        result = cursor.fetchone()
        if result:
            if result[0] and result[1] and result[2]:
                used_instances = int(result[0])
                used_cpus = int(result[1])
                used_memory = int(result[2])
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in get_used_resources due to: %s', err)
    return (used_instances, used_cpus, used_memory)

def set_deployment_stats(self, unique_infra_id, reason):
    """
    Set deployment failure reason
    """
    return self.execute("UPDATE deployment_log SET reason=%s WHERE unique_infra_id='%s'" % (reason, unique_infra_id))
