import logging
import time

# Logging
logger = logging.getLogger(__name__)

def set_cloud_updated_images(self, cloud, identity):
    """
    """
    return self.execute("UPDATE clouds_info SET updated_images=%s WHERE identity='%s' AND name='%s'" % (time.time(), identity, cloud))

def get_cloud_updated_images(self, cloud, identity):
    """
    """
    updated = 0
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT updated_images FROM clouds_info WHERE identity='%s' AND name='%s'" % (identity, cloud))
        for row in cursor:
            updated = row[0]
        cursor.close()
    except Exception as error:
        logger.critical('[get_cloud_updated_images] Unable to execute SELECT query due to: %s', error)

    return updated

def get_images(self, identity, cloud):
    """
    """
    results = {}

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT name, im_name, os_type, os_arch, os_dist, os_vers FROM cloud_images WHERE identity='%s' AND cloud='%s'" % (identity, cloud))
        for row in cursor:
            data = {"name": row[0],
                    "im_name": row[1],
                    "type": row[2],
                    "architecture": row[3],
                    "distribution": row[4],
                    "version": row[5]}
            results[row[0]] = data
        cursor.close()
    except Exception as error:
        logger.critical('[get_images] unable to execute SELECT query due to: %s', error)

    return results

def get_image(self, identity, cloud, os_type, os_arch, os_dist, os_vers):
    """
    """
    name = None
    im_name = None
    use_identity = "identity='%s'"
    if identity != 'static':
        use_identity = "(identity='%s' OR identity='static')"

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT name, im_name FROM cloud_images WHERE %s AND cloud='%s' AND os_type='%s' AND os_arch='%s' AND os_dist='%s' AND os_vers='%s' ORDER BY name ASC" % (use_identity, cloud, os_type, os_arch, os_dist, os_vers))
        for row in cursor:
            name = row[0]
            im_name = row[1]
        cursor.close()
    except Exception as error:
        logger.critical('[get_image] unable to execute SELECT query due to: %s', error)

    return name, im_name

def set_image(self, identity, cloud, name, im_name, os_type, os_arch, os_dist, os_vers):
    """
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM cloud_images WHERE identity='%s' AND cloud='%s' AND name='%s'" % (identity, cloud, name))
        cursor.execute("INSERT INTO cloud_images (identity, cloud, name, im_name, os_type, os_arch, os_dist, os_vers) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (identity, cloud, name, im_name, os_type, os_arch, os_dist, os_vers))
        self._connection.commit()
        cursor.close()
    except Exception as error:
        logger.critical('[set flavour] unable to execute DELETE+INSERT query due to: %s', error)
        return False

    return True

def delete_image(self, identity, cloud, name):
    """
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM cloud_images WHERE identity='%s' AND cloud='%s' AND name='%s'" % (identity, cloud, name))
        self._connection.commit()
        cursor.close()
    except Exception as error:
        logger.critical('[delete_image] unable to execute DELETE query due to: %s', error)
        return False

    return True
