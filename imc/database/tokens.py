import logging

# Logging
logger = logging.getLogger(__name__)

def update_token(self, cloud, token, expiry, creation):
    """
    Update token in the DB
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("UPDATE credentials SET token='%s',expiry=%d,creation=%d WHERE cloud='%s'" % (token, expiry, creation, cloud))
        cursor.execute("INSERT INTO credentials (cloud, token, expiry, creation) SELECT '%s', '%s', '%s', '%s' WHERE NOT EXISTS (SELECT 1 FROM credentials WHERE cloud='%s')" % (cloud, token, expiry, creation, cloud))
        self._connection.commit()
        cursor.close()
    except Exception as error:
        logger.critical('[update_token] Unable to execute UPDATE or INSERT query due to: %s', error)
        return False

    return True

def set_user_credentials(self, identity, refresh_token):
    """
    Insert or update user credentials
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("UPDATE user_credentials SET refresh_token='%s' WHERE identity='%s';" % (refresh_token, identity))
        cursor.execute("INSERT INTO user_credentials (identity, access_token, refresh_token, access_token_creation, access_token_expiry) SELECT '%s', '%s', '%s', %d, %d WHERE NOT EXISTS (SELECT 1 FROM user_credentials WHERE identity='%s');" % (identity, '', refresh_token, -1, -1, identity))
        self._connection.commit()
        cursor.close()
    except Exception as error:
        logger.critical('[set_user_credentials] Unable to execute UPDATE or INSERT query due to: %s', error)
        return False
    return True

def update_user_access_token(self, identity, access_token, expiry, creation):
    """
    Update user access token
    """
    return self.execute("UPDATE user_credentials SET access_token='%s',access_token_creation=%d,access_token_expiry=%d WHERE identity='%s'" % (access_token, creation, expiry, identity))

def get_user_credentials(self, identity):
    """
    Get user credentials
    """
    refresh_token = None
    access_token = None
    access_token_creation = -1
    access_token_expiry = -1

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT refresh_token, access_token, access_token_creation, access_token_expiry FROM user_credentials WHERE identity='%s'" % identity)
        result = cursor.fetchone()
        refresh_token = result[0]
        access_token = result[1]
        access_token_creation = result[2]
        access_token_expiry = result[3]
        cursor.close()
    except Exception as error:
        logger.critical('[get_user_credentials] Unable to execute SELECT query due to: %s', error)
    return (refresh_token, access_token, access_token_creation, access_token_expiry)

def get_token(self, cloud):
    """
    Get a token & expiry date for the specified cloud
    """
    token = None
    expiry = -1
    creation = -1

    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT token,expiry,creation FROM credentials WHERE cloud='%s'" % cloud)
        result = cursor.fetchone()
        token = result[0]
        expiry = result[1]
        creation = result[2]
        cursor.close()
    except Exception as error:
        logger.critical('[get_token] Unable to execute SELECT query due to: %s', error)
    return (token, expiry, creation)

def delete_token(self, cloud):
    """
    Delete a token for the specified cloud
    """
    return self.execute("DELETE FROM credentials WHERE cloud='%s'" % cloud)
