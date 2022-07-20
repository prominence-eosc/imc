import logging
import time
from psycopg2.extras import Json

# Logging
logger = logging.getLogger(__name__)

def create_resource(self, identity, name, description):
    """
    Create resource
    """
    if 'supported_identities' not in description:
        description['supported_identities'] = [identity]

    return self.execute("INSERT INTO resources (identity,name,description) VALUES (%s,%s,%s)", (identity, name, Json(description)))

def list_resources(self, identity=None):
    """
    List resources
    """
    query = "identity='admin'"
    if identity:
        query = "%s or identity='%s'" % (query, identity)

    resources = []
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT description FROM resources WHERE %s" % query)
        for row in cursor:
            resources.append(row[0])
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in list_resources due to: %s', err)

    return resources

def describe_resource(self, identity, name):
    """
    Describe resource
    """
    pass

def delete_resource(self, identity, name):
    """
    Delete resource
    """
    pass
