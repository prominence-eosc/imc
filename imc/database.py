from __future__ import print_function
import configparser
import logging
import os
import sys
import time
import psycopg2
from psycopg2.extras import Json

from imc import config
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def get_db():
    """
    Prepare DB
    """
    db = Database(CONFIG.get('db', 'host'),
                  CONFIG.get('db', 'port'),
                  CONFIG.get('db', 'db'),
                  CONFIG.get('db', 'username'),
                  CONFIG.get('db', 'password'))
    return db

class Database(object):
    """
    Database helper
    """

    def __init__(self, host=None, port=None, db=None, username=None, password=None):
        self._host = host
        self._db = db
        self._port = port
        self._username = username
        self._password = password
        self._connection = None

    def init(self):
        """
        Initialize database
        """
        # Connect to the DB
        self.connect()

        # Setup tables if necessary
        try:
            cursor = self._connection.cursor()

            # Create Ansible nodes table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              ansible_nodes(cloud TEXT NOT NULL PRIMARY KEY,
                                            infrastructure_id TEXT NOT NULL,
                                            public_ip TEXT NOT NULL,
                                            username TEXT NOT NULL,
                                            creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                            last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                            )''')

            # Create user credentials table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              user_credentials(identity TEXT NOT NULL PRIMARY KEY,
                                               refresh_token TEXT NOT NULL,
                                               access_token TEXT NOT NULL,
                                               access_token_creation INT NOT NULL,
                                               access_token_expiry INT NOT NULL)''')

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
                                          description JSON NOT NULL,
                                          status TEXT NOT NULL,
                                          status_reason TEXT,
                                          im_infra_id TEXT,
                                          cloud TEXT,
                                          resource_type TEXT,
                                          identity TEXT,
                                          identifier TEXT,
                                          creation INT NOT NULL,
                                          updated INT NOT NULL
                                          )''')

            self._connection.commit()
        except Exception as error:
            logger.critical('Unable to initialize the database due to: %s', error)

        # Close the DB connection
        self.close()

    def connect(self):
        """
        Connect to the DB
        """
        try:
            self._connection = psycopg2.connect(user=self._username,
                                                password=self._password,
                                                host=self._host,
                                                port=self._port,
                                                database=self._db)
        except Exception as error:
            logger.critical('Unable to connect to the database due to: %s', error)

        if self._connection:
            return True
        return False

    def close(self):
        """
        Close the connection to the DB
        """
        try:
            self._connection.close()
        except Exception as error:
            logger.critical('Unable to the database connection due to: %s', error)

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
            cursor.execute("SELECT id,creation,updated FROM deployments WHERE status='%s' %s" % (state, query))
            for row in cursor:
                infra.append({"id":row[0], "created":row[1], "updated":row[2]})
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

    def deployment_create(self, infra_id, description, identity, identifier):
        """
        Create deployment
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute("INSERT INTO deployments (id,description,status,identity,identifier,creation,updated) VALUES (%s,%s,'accepted',%s,%s,%s,%s)", (infra_id, Json(description), identity, identifier, time.time(), time.time()))
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[deployment_create] Unable to execute INSERT query due to: %s', error)
            return False
        return True

    def deployment_remove(self, infra_id):
        """
        Remove an infrastructure from the DB
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute("DELETE FROM deployments WHERE id='%s'" % infra_id)
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[deployment_remove] Unable to execute DELETE query due to: %s', error)
            return False
        return True

    def deployment_update_status_with_retries(self, infra_id, status=None, cloud=None, im_infra_id=None, resource_type='cloud'):
        """
        Update deployment status with retries
        """
        max_retries = 10
        count = 0
        success = False
        while count < max_retries and not success:
            success = self.deployment_update_status(infra_id, status, cloud, im_infra_id, resource_type)
            if not success:
                count += 1
                self.close()
                time.sleep(count/2)
                self.connect()
        return success

    def deployment_update_status(self, infra_id, status=None, cloud=None, im_infra_id=None, resource_type='cloud'):
        """
        Update deployment status
        """
        try:
            cursor = self._connection.cursor()
            if cloud and im_infra_id and status:
                cursor.execute("UPDATE deployments SET resource_type='%s',status='%s',cloud='%s',im_infra_id='%s',updated=%d WHERE id='%s'" % (resource_type, status, cloud, im_infra_id, time.time(), infra_id))
            elif cloud and status:
                cursor.execute("UPDATE deployments SET resource_type='%s',status='%s',cloud='%s',updated=%d WHERE id='%s'" % (resource_type, status, cloud, time.time(), infra_id))
            elif im_infra_id and cloud and not status:
                cursor.execute("UPDATE deployments SET resource_type='%s',cloud='%s',im_infra_id='%s',updated=%d WHERE id='%s'" % (resource_type, cloud, im_infra_id, time.time(), infra_id))
            elif status:
                if status in ('configured', 'waiting', 'unable', 'creating'):
                    cursor.execute("UPDATE deployments SET resource_type='%s',status='%s',updated=%d WHERE id='%s' AND status NOT IN ('deleted', 'deleting', 'deletion-requested', 'deletion-failed')" % (resource_type, status, time.time(), infra_id))
                else:
                    cursor.execute("UPDATE deployments SET resource_type='%s',status='%s',updated=%d WHERE id='%s'" % (resource_type, status, time.time(), infra_id))
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[db_deployment_update_status] Unable to execute UPDATE query due to: %s', error)
            return False
        return True

    def deployment_update_status_reason_with_retries(self, infra_id, status_reason):
        """
        Update deployment status reason with retries
        """
        max_retries = 10
        count = 0
        success = False
        while count < max_retries and not success:
            success = self.deployment_update_status_reason(infra_id, status_reason)
            if not success:
                count += 1
                self.close()
                time.sleep(count/2)
                self.connect()
        return success

    def deployment_update_status_reason(self, infra_id, status_reason):
        """
        Update deploymeny status reason
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute("UPDATE deployments SET status_reason='%s' WHERE id='%s'" % (status_reason, infra_id))
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[db_deployment_update_status_reason] Unable to execute UPDATE query due to: %s', error)
            return False
        return True

    def set_token(self, cloud, token, expiry, creation):
        """
        Write token to the DB
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute("INSERT INTO credentials (cloud, token, expiry, creation) VALUES (%s, %s, %s, %s)", (cloud, token, expiry, creation))
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[set_token] Unable to execute INSERT query due to: %s', error)
            return False
        return True

    def set_ansible_node(self, cloud, infrastructure_id, public_ip, username):
        """
        Write Ansible node details to DB
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute("INSERT INTO ansible_nodes (cloud, infrastructure_id, public_ip, username) VALUES (%s, %s, %s, %s)", (cloud, infrastructure_id, public_ip, username))
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[set_ansible_node] Unable to execute INSERT query due to: %s', error)
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
        try:
            cursor = self._connection.cursor()
            cursor.execute("UPDATE user_credentials SET access_token='%s',access_token_creation=%d,access_token_expiry=%d WHERE identity='%s'" % (access_token, creation, expiry, identity))
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[update_user_access_token] Unable to execute UPDATE query due to: %s', error)
            return False
        return True

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
            for row in cursor:
                refresh_token = row[0]
                access_token = row[1]
                access_token_creation = row[2]
                access_token_expiry = row[3]
            cursor.close()
        except Exception as error:
            logger.critical('[get_user_credentials] Unable to execute SELECT query due to: %s', error)
            return (refresh_token, access_token, access_token_creation, access_token_expiry)
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
            for row in cursor:
                token = row[0]
                expiry = row[1]
                creation = row[2]
            cursor.close()
        except Exception as error:
            logger.critical('[get_token] Unable to execute SELECT query due to: %s', error)
            return (token, expiry, creation)
        return (token, expiry, creation)

    def delete_ansible_node(self, cloud):
        """
        Delete an Ansible node for the specified cloud
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute("DELETE FROM ansible_nodes WHERE cloud='%s'" % cloud)
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[delete_ansible_node] Unable to execute DELETE query due to: %s', error)
            return False
        return True

    def delete_token(self, cloud):
        """
        Delete a token for the specified cloud
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute("DELETE FROM credentials WHERE cloud='%s'" % cloud)
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[delete_token] Unable to execute DELETE query due to: %s', error)
            return False
        return True
