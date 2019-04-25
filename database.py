#!/usr/bin/python
from __future__ import print_function
import logging
import sqlite3
import sys
import time

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)
    
class Database(object):
    """
    Database helper
    """
    
    def __init__(self, file=None):
        self._file = file
    
    def init(self):
        """
        Initialize database
        """
    
        # Setup database table if necessary
        try:
            db = sqlite3.connect(self._file)
            cursor = db.cursor()
    
            # Create Ansible nodes table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              ansible_nodes(cloud TEXT NOT NULL PRIMARY KEY,
                                            infrastructure_id TEXT NOT NULL,
                                            public_ip TEXT NOT NULL,
                                            username TEXT NOT NULL,
                                            creation DATETIME DEFAULT CURRENT_TIMESTAMP,
                                            last_used DATETIME DEFAULT CURRENT_TIMESTAMP
                                            )''')
    
            # Create credentials table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              credentials(cloud TEXT NOT NULL PRIMARY KEY,
                                          token TEXT NOT NULL,
                                          expiry INT NOT NULL,
                                          creation INT NOT NULL
                                          )''')
    
            # Create deployments table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              deployments(id TEXT NOT NULL PRIMARY KEY,
                                          status TEXT NOT NULL,
                                          im_infra_id TEXT,
                                          cloud TEXT,
                                          creation INT NOT NULL
                                          )''')
    
    
            db.commit()
            db.close()
        except Exception as ex:
            print(ex)
            exit(1)
    
    def connect(self):
        """
        Connect to the DB
        """
        try:
            self._db = sqlite3.connect(self._file)
        except Exception as ex:
            logger.critical('[db_connect] Unable to connect to sqlite DB because of %s', ex)
            return 1
        return 0
    
    def close(self):
        """
        Close the connection to the DB
        """
        self._db.close()
    
    def deployment_get_im_infra_id(self, infra_id):
        im_infra_id = None
        status = None
        cloud = None
    
        try:
            cursor = self._db.cursor()
            cursor.execute('SELECT im_infra_id,status,cloud FROM deployments WHERE id="%s"' % infra_id)
    
            for row in cursor:
                im_infra_id = row[0]
                status = row[1]
                cloud = row[2]
    
        except Exception as ex:
            logger.critical('[db_get] Unable to connect to sqlite DB because of %s', ex)
    
        return (im_infra_id, status, cloud)
    
    def deployment_create_with_retries(self, infra_id):
        """
        Create deployment with retries & backoff
        """
        max_retries = 10
        count = 0
        success = False
        while count < max_retries and not success:
            success = self.deployment_create(infra_id)
            if not success:
                count += 1
                self.close()
                time.sleep(count/2)
                self.connect()
        return success
    
    def deployment_create(self, infra_id):
        """
        Create deployment
        """
        try:
            cursor = self._db.cursor()
            cursor.execute('INSERT INTO deployments (id,status,creation) VALUES ("%s","accepted",%d)' % (infra_id, time.time()))
            self._db.commit()
        except Exception as e:
            logger.critical('[db_set] Unable to connect to sqlite DB because of %s', e)
            return False
        return True
    
    def deployment_update_infra_with_retries(self, infra_id):
        """
        Create deployment with retries & backoff
        """
        max_retries = 10
        count = 0
        success = False
        while count < max_retries and not success:
            success = self.deployment_update_infra(infra_id)
            if not success:
                count += 1
                self.close()
                time.sleep(count/2)
                self.connect()
        return success
    
    def deployment_update_infra(self, infra_id):
        """
        Update deployment with IM infra id
        """
        try:
            cursor = self._db.cursor()
            cursor.execute('UPDATE deployments SET status="creating" WHERE id="%s"' % infra_id)
            self._db.commit()
        except Exception as e:
            logger.critical('[db_deployment_update_infra] Unable to connect to sqlite DB because of %s', e)
            return False
        return True
    
    def deployment_update_status_with_retries(self, infra_id, status, cloud=None, im_infra_id=None):
        """
        Update deployment status with retries
        """
        max_retries = 10
        count = 0
        success = False
        while count < max_retries and not success:
            success = self.deployment_update_status(infra_id, status, cloud, im_infra_id)
            if not success:
                count += 1
                self.close()
                time.sleep(count/2)
                self.connect()
        return success
    
    def deployment_update_status(self, id, status, cloud=None, im_infra_id=None):
        """
        Update deployment status
        """
        try:
            cursor = self._db.cursor()
            if cloud is not None and im_infra_id is not None:
                cursor.execute('UPDATE deployments SET status="%s",cloud="%s",im_infra_id="%s" WHERE id="%s"' % (status, cloud, im_infra_id, id))
            elif cloud is not None:
                cursor.execute('UPDATE deployments SET status="%s",cloud="%s" WHERE id="%s"' % (status, cloud, id))
            else:
                cursor.execute('UPDATE deployments SET status="%s" WHERE id="%s"' % (status, id))
            self._db.commit()
        except Exception as e:
            logger.critical('[db_deployment_update_status] Unable to connect to sqlite DB because of %s', e)
            return False
        return True
    
    def set_token(self, cloud, token, expiry, creation):
        """
        Write token to the DB
        """
        try:
            cursor = self._db.cursor()
            cursor.execute('INSERT INTO credentials (cloud, token, expiry, creation) VALUES ("%s", "%s", %d, %d)' % (cloud, token, expiry, creation))
            self._db.commit()
        except Exception as e:
            logger.critical('[db_set] Unable to connect to sqlite DB because of %s', e)
            return False
        return True
    
    def set_ansible_node(self, cloud, infrastructure_id, public_ip, username):
        """
        Write Ansible node details to DB
        """
        try:
            cursor = self._db.cursor()
            cursor.execute('INSERT INTO ansible_nodes (cloud, infrastructure_id, public_ip, username) VALUES ("%s", "%s", "%s", "%s")' % (cloud, infrastructure_id, public_ip, username))
            self._db.commit()
        except Exception as e:
            logger.critical('[db_set] Unable to connect to sqlite DB because of %s', e)
            return False
        return True
    
    def get_ansible_node(self, cloud):
        """
        Get details about an Ansible node for the specified cloud
        """
        infrastructure_id = None
        public_ip = None
        username = None
        timestamp = None
    
        try:
            cursor = self._db.cursor()
            cursor.execute('SELECT infrastructure_id, public_ip, username, creation FROM ansible_nodes WHERE cloud="%s"' % cloud)
    
            for row in cursor:
                infrastructure_id = row[0]
                public_ip = row[1]
                username = row[2]
                timestamp = row[3]
    
        except Exception as e:
            logger.critical('[db_get] Unable to connect to sqlite DB because of %s', e)
    
        return (infrastructure_id, public_ip, username, timestamp)
    
    def get_token(self, cloud):
        """
        Get a token & expiry date for the specified cloud
        """
        token = None
        expiry = -1
        creation = -1
    
        try:
            cursor = self._db.cursor()
            cursor.execute('SELECT token,expiry,creation FROM credentials WHERE cloud="%s"' % cloud)
    
            for row in cursor:
                token = row[0]
                expiry = row[1]
                creation = row[2]
    
        except Exception as e:
            logger.critical('[db_get] Unable to connect to sqlite DB because of %s', e)
            return (token, expiry, creation)
    
        return (token, expiry, creation)
    
    def delete_ansible_node(self, cloud):
        """
        Delete an Ansible node for the specified cloud
        """
        try:
            cursor = self._db.cursor()
            cursor.execute('DELETE FROM ansible_nodes WHERE cloud="%s"' % cloud)
            self._db.commit()
        except Exception as e:
            logger.critical('[db_delete] Unable to connect to sqlite DB because of %s', e)
            return False
        return True
    
    def delete_token(self, cloud):
        """
        Delete a token for the specified cloud
        """
        try:
            cursor = self._db.cursor()
            cursor.execute('DELETE FROM credentials WHERE cloud="%s"' % cloud)
            self._db.commit()
        except Exception as e:
            logger.critical('[db_delete] Unable to connect to sqlite DB because of %s', e)
            return False
        return True
    
