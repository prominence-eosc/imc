import logging
import time

# Logging
logger = logging.getLogger(__name__)

def set_ansible_node(self, cloud, infrastructure_id, public_ip, username):
    """
    Write Ansible node details to DB
    """
    return self.execute("INSERT INTO ansible_nodes (cloud, infrastructure_id, public_ip, username) VALUES (%s, %s, %s, %s)", (cloud, infrastructure_id, public_ip, username))

def get_ansible_node(self, cloud):
    """
    Get details about an Ansible node for the specified cloud
    """
    infrastructure_id = None
    public_ip = None
    username = None
    timestamp = None

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT infrastructure_id, public_ip, username, creation FROM ansible_nodes WHERE cloud='%s'" % cloud)
        for row in cursor:
            infrastructure_id = row[0]
            public_ip = row[1]
            username = row[2]
            timestamp = row[3]
        cursor.close()
    except Exception as error:
        logger.critical('[get_ansible_node] Unable to execute SELECT query due to: %s', error)
    return (infrastructure_id, public_ip, username, timestamp)

def delete_ansible_node(self, cloud):
    """
    Delete an Ansible node for the specified cloud
    """
    return self.execute("DELETE FROM ansible_nodes WHERE cloud='%s'" % cloud)
