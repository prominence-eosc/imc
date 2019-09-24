from __future__ import print_function
import logging
import sys
import time
import psycopg2

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

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
                                          status_reason TEXT,
                                          im_infra_id TEXT,
                                          cloud TEXT,
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

    def deployment_get_infra_in_state_cloud(self, state, cloud=None):
        """
        Return a list of all infrastructure IDs for infrastructure in the specified state and cloud
        """
        query = ""
        if cloud is not None:
            query = "and cloud='%s'" % cloud
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
            cursor = self._connection.cursor()
            cursor.execute("INSERT INTO deployments (id,status,creation,updated) VALUES (%s,'accepted',%s,%s)", (infra_id, time.time(), time.time()))
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('[deployment_create] Unable to execute INSERT query due to: %s', error)
            return False
        return True

    def deployment_update_status_with_retries(self, infra_id, status=None, cloud=None, im_infra_id=None):
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

    def deployment_update_status(self, infra_id, status=None, cloud=None, im_infra_id=None):
        """
        Update deployment status
        """
        try:
            cursor = self._connection.cursor()
            if cloud is not None and im_infra_id is not None and status is not None:
                cursor.execute("UPDATE deployments SET status='%s',cloud='%s',im_infra_id='%s',updated=%d WHERE id='%s'" % (status, cloud, im_infra_id, time.time(), infra_id))
            elif cloud is not None and status is not None:
                cursor.execute("UPDATE deployments SET status='%s',cloud='%s',updated=%d WHERE id='%s'" % (status, cloud, time.time(), infra_id))
            elif im_infra_id is not None and cloud is not None and status is None:
                cursor.execute("UPDATE deployments SET cloud='%s',im_infra_id='%s',updated=%d WHERE id='%s'" % (cloud, im_infra_id, time.time(), infra_id))
            elif status is not None:
                cursor.execute("UPDATE deployments SET status='%s',updated=%d WHERE id='%s'" % (status, time.time(), infra_id))
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
