import logging

# Logging
logger = logging.getLogger(__name__)

def get_all_flavours(self, identity, cloud):
    """
    Return all flavours for a specific cloud
    """
    results = {}

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT name, cpus, memory, disk FROM cloud_flavours WHERE identity='%s' AND cloud='%s'" % (identity, cloud))  
        for row in cursor:
            data = {"name": row[0],
                    "cpus": row[1],
                    "memory": row[2],
                    "disk": row[3]}
            results[row[0]] = data
        cursor.close()
    except Exception as error:
        logger.critical('[get_flavours] unable to execute SELECT query due to: %s', error)

    return results

def get_flavours(self, identity, cloud, cpus, memory, disk):
    """
    Return all flavours which can provide the specified resources
    """
    flavours = []
    use_identity = "identity='%s'"
    if identity != 'static':
        use_identity = "(identity='%s' OR identity='static')" % identity

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT id, name, cpus, memory, disk FROM cloud_flavours WHERE %s AND cloud='%s' AND cpus>=%s AND memory>=%s AND (disk>=%s OR disk=-1) ORDER BY cpus*memory ASC" % (use_identity, cloud, cpus, memory, disk))
        for row in cursor:
            flavours.append((row[0], row[1], int(row[2]), int(row[3]), int(row[4])))
        cursor.close()
    except Exception as error:
        logger.critical('[get_flavours] unable to execute SELECT query due to: %s', error)

    return flavours

def get_flavour(self, identity, cloud, cpus, memory, disk):
    """
    Return a single flavour of smallest size which can provide the specified resources
    """
    name = None
    cpus_used = -1
    memory_used = -1
    disk_used = -1

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT name, cpus, memory, disk FROM cloud_flavours WHERE identity='%s' AND cloud='%s' AND cpus>=%s AND memory>=%s AND (disk>=%s OR disk=-1) ORDER BY cpus*memory ASC LIMIT 1" % (identity, cloud, cpus, memory, disk))
        for row in cursor:
            name = row[0]
            cpus_used = int(row[1])
            memory_used = int(row[2])
            disk_used = int(row[3])
        cursor.close()
    except Exception as error:
        logger.critical('[get_flavour] unable to execute SELECT query due to: %s', error)

    return (name, cpus_used, memory_used, disk_used)

def set_flavour(self, identity, cloud, name, id, cpus, memory, disk):
    """
    Add a new flavour
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM cloud_flavours WHERE identity='%s' AND cloud='%s' AND name='%s'" % (identity, cloud, name))
        cursor.execute("INSERT INTO cloud_flavours (identity, cloud, name, id, cpus, memory, disk) VALUES (%s, %s, %s, %s, %s, %s, %s)", (identity, cloud, name, id, cpus, memory, disk))
        self._connection.commit()
        cursor.close()
    except Exception as error:
        logger.critical('[set flavour] unable to execute DELETE+INSERT query due to: %s', error)
        return False

    return True

def delete_flavour(self, identity, cloud, name):
    """
    Delete a flavour
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM cloud_flavours WHERE identity='%s' AND cloud='%s' AND name='%s'" % (identity, cloud, name))
        self._connection.commit()
        cursor.close()
    except Exception as error:
        logger.critical('[delete_flavour] unable to execute DELETE query due to: %s', error)
        return False

    return True
