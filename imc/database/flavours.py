import logging

# Logging
logger = logging.getLogger(__name__)

def get_flavours(self, identity, cloud):
    """
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

def get_flavour(self, identity, cloud, cpus, memory, disk):
    """
    """
    name = None
    cpus_used = -1
    memory_used = -1
    disk_used = -1

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT name, cpus, memory, disk FROM cloud_flavours WHERE identity='%s' AND cloud='%s' AND cpus>=%s AND memory>=%s AND disk>=%s ORDER BY cpus*memory ASC LIMIT 1" % (identity, cloud, cpus, memory, disk))
        for row in cursor:
            name = row[0]
            cpus_used = int(row[1])
            memory_used = int(row[2])
            disk_used = int(row[3])
        cursor.close()
    except Exception as error:
        logger.critical('[get_flavour] unable to execute SELECT query due to: %s', error)

    return (name, cpus_used, memory_used, disk_used)

def set_flavour(self, identity, cloud, name, cpus, memory, disk): #TODO: do as single SQL statement
    """
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM cloud_flavours WHERE identity='%s' AND cloud='%s' AND name='%s'" % (identity, cloud, name))
        cursor.execute("INSERT INTO cloud_flavours (identity, cloud, name, cpus, memory, disk) VALUES (%s, %s, %s, %s, %s, %s)", (identity, cloud, name, cpus, memory, disk))
        self._connection.commit()
        cursor.close()
    except Exception as error:
        logger.critical('[set flavour] unable to execute DELETE+INSERT query due to: %s', error)
        return False

    return True
