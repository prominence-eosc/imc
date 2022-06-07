import logging
import time

# Logging
logger = logging.getLogger(__name__)

def set_cloud_updated_images(self, cloud, identity):
    """
    Set time when images updated
    """
    return self.execute("UPDATE status SET updated_images=%s WHERE identity='%s' AND name='%s'" % (time.time(), identity, cloud))

def get_cloud_updated_images(self, cloud, identity):
    """
    Get time when images were last updated
    """
    updated = 0
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT updated_images FROM status WHERE identity='%s' AND name='%s'" % (identity, cloud))
        result = cursor.fetchone()
        if result:
            updated = result[0]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in get_cloud_updated_images due to: %s', err)

    return updated

def get_images(self, identity, cloud):
    """
    Return all images associated with the specified cloud
    """
    results = {}

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT name, id, os_type, os_arch, os_dist, os_vers FROM images WHERE identity='%s' AND cloud='%s'" % (identity, cloud))
        for row in cursor:
            data = {"name": row[0],
                    "id": row[1],
                    "type": row[2],
                    "architecture": row[3],
                    "distribution": row[4],
                    "version": row[5]}
            results[row[0]] = data
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in get_images due to: %s', err)

    return results

def get_image(self, identity, cloud, os_type, os_arch, os_dist, os_vers):
    """
    Get an image from the specified cloud and requirements
    """
    name = None
    im_name = None
    use_identity = "identity='%s'" % identity
    if identity != 'static':
        use_identity = "(identity='%s' OR identity='static')" % identity

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT name, id FROM images WHERE %s AND cloud='%s' AND os_type='%s' AND os_arch='%s' AND os_dist='%s' AND os_vers='%s' ORDER BY name ASC" % (use_identity, cloud, os_type, os_arch, os_dist, os_vers))
        result = cursor.fetchone()
        if result:
            name = result[0]
            im_name = result[1]
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in get_image due to: %s', err)

    return name, im_name

def set_image(self, identity, cloud, name, id, os_type, os_arch, os_dist, os_vers):
    """
    Set an image
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM images WHERE identity='%s' AND cloud='%s' AND name='%s'" % (identity, cloud, name))
        cursor.execute("INSERT INTO images (identity, cloud, name, id, os_type, os_arch, os_dist, os_vers) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (identity, cloud, name, id, os_type, os_arch, os_dist, os_vers))
        self._connection.commit()
        cursor.close()
    except Exception as err:
        logger.critical('Unable to execute query in set_image due to: %s', err)
        return False

    return True

def delete_image(self, identity, cloud, name):
    """
    Delete an image
    """
    return self.execute("DELETE FROM images WHERE identity='%s' AND cloud='%s' AND name='%s'" % (identity, cloud, name))
