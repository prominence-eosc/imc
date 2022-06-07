import logging
import time
import psycopg2
from psycopg2.extras import Json

from imc import config
from imc import utilities

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

# Numbers of retries
DATABASE_CONNECTION_MAX_RETRIES = 5
DATABASE_QUERY_MAX_RERIES = 5

def get_db():
    """
    Database helper function
    """
    db = Database(CONFIG.get('db', 'host'),
                  CONFIG.get('db', 'port'),
                  CONFIG.get('db', 'db'),
                  CONFIG.get('db', 'username'),
                  CONFIG.get('db', 'password'))
    return db

class Database(object):
    """
    Database access
    """
    from .flavours import get_flavour, \
                          set_flavour, \
                          get_flavours, \
                          get_all_flavours, \
                          delete_flavour

    from .images import set_cloud_updated_images, \
                        get_cloud_updated_images, \
                        get_images, \
                        get_image, \
                        set_image, \
                        delete_image

    from .tokens import update_token, \
                        set_user_credentials, \
                        update_user_access_token, \
                        get_user_credentials, \
                        get_token, \
                        delete_token

    from .deployment import deployment_get_infra_in_state_cloud, \
                            deployment_check_infra_id, \
                            deployment_get_status_reason, \
                            deployment_get_identity, \
                            deployment_get_identities, \
                            deployment_get_json, \
                            deployment_get_infra_id, \
                            deployment_create, \
                            deployment_remove, \
                            deployment_log_remove, \
                            deployment_update_status, \
                            deployment_update_status_reason, \
                            deployment_update_resources, \
                            get_infra_from_infra_id, \
                            get_deployment, \
                            set_deployment_stats, \
                            create_cloud_deployment, \
                            update_cloud_deployment, \
                            get_deployments, \
                            get_used_resources

    from .egi import set_egi_cloud, \
                     get_egi_clouds, \
                     disable_egi_clouds

    from .quotas import set_cloud_static_quotas, \
                        set_cloud_dynamic_quotas, \
                        set_cloud_updated_quotas, \
                        get_cloud_updated_quotas

    from .clouds import get_cloud_info, \
                        set_cloud_updated_quotas, \
                        set_cloud_mon_status, \
                        set_cloud_status, \
                        init_cloud_info, \
                        get_deployment_stats, \
                        del_old_deployment_stats, \
                        set_resources_update, \
                        get_resources_update, \
                        set_resources_update_start

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
        if not self.connect():
            logger.critical('Unable to connect to the database, cannot check or create tables')
            return

        # Setup tables if necessary
        try:
            cursor = self._connection.cursor()

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
                                          cloud_infra_id TEXT,
                                          cloud TEXT,
                                          resource_type TEXT,
                                          identity TEXT,
                                          identifier TEXT,
                                          creation INT NOT NULL,
                                          updated INT NOT NULL,
                                          used_cpus INT NOT NULL DEFAULT 0,
                                          used_memory INT NOT NULL DEFAULT 0,
                                          used_instances INT NOT NULL DEFAULT 0
                                          )''')

            # Create deployments log table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              deployment_log(unique_infra_id TEXT NOT NULL PRIMARY KEY,
                                             cloud TEXT NOT NULL,
                                             cloud_infra_id TEXT,
                                             id TEXT NOT NULL,
                                             created INT NOT NULL,
                                             CONSTRAINT fk_infra
                                             FOREIGN KEY(id)
                                             REFERENCES deployments(id)
                                             )''')

            # Create deployment stats table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              deployment_stats(cloud TEXT NOT NULL,
                                               identity TEXT NOT NULL,
                                               reason INT NOT NULL,
                                               time INT NOT NULL,
                                               duration INT DEFAULT -1
                                               )''')
            # Reasons:
            # 0 = Success
            # 1 = VMs failed
            # 2 = Waiting too long to start running
            # 5 = Total time waiting too long
            # 6 = Quota exceeded
            # 7 = Image not found or not active
            # 8 = Flavor not found
            # 9 = Insufficient capacity

            # Create cloud updates table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              updates(identity TEXT NOT NULL PRIMARY KEY,
                                      start INT NOT NULL DEFAULT 0,
                                      time INT NOT NULL DEFAULT 0
                                            )''')

            # Create egi_clouds table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              egi(identity TEXT NOT NULL,
                                  site TEXT NOT NULL,
                                  auth_url TEXT NOT NULL,
                                  project_id TEXT NOT NULL,
                                  project_domain_id TEXT NOT NULL,
                                  user_domain_name TEXT NOT NULL,
                                  region TEXT NOT NULL,
                                  protocol TEXT NOT NULL,
                                  enabled BOOLEAN DEFAULT TRUE,
                                  PRIMARY KEY (site, identity, project_id)
                                  )''')

            # Create cloud status
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              status(name TEXT NOT NULL,
                                     identity TEXT NOT NULL,
                                     status INT NOT NULL DEFAULT -1,
                                     mon_status INT NOT NULL DEFAULT -1,
                                     limit_cpus INT NOT NULL DEFAULT -1,
                                     limit_memory INT NOT NULL DEFAULT -1,
                                     limit_instances INT NOT NULL DEFAULT -1,
                                     remaining_cpus INT NOT NULL DEFAULT -1,
                                     remaining_memory INT NOT NULL DEFAULT -1,
                                     remaining_instances INT NOT NULL DEFAULT -1,
                                     updated_quotas INT NOT NULL DEFAULT -1,
                                     updated_images INT NOT NULL DEFAULT -1,
                                     PRIMARY KEY (name, identity)
                                     )''')

            # Create cloud flavours
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              flavours(id TEXT NOT NULL,
                                       name TEXT NOT NULL,
                                       cpus INT NOT NULL,
                                       memory INT NOT NULL,
                                       disk INT NOT NULL,
                                       limit_instances INT NOT NULL DEFAULT -1,
                                       cloud TEXT NOT NULL,
                                       identity TEXT NOT NULL,
                                       PRIMARY KEY (name, cloud, identity)
                                       )''')

            # Create cloud images
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              images(name TEXT NOT NULL,
                                     id TEXT NOT NULL,
                                     os_arch TEXT NOT NULL,
                                     os_dist TEXT NOT NULL,
                                     os_type TEXT NOT NULL,
                                     os_vers TEXT NOT NULL,
                                     cloud TEXT NOT NULL,
                                     identity TEXT NOT NULL,
                                     PRIMARY KEY (name, cloud, identity)
                                     )''')
                                          
            self._connection.commit()
            cursor.close()
        except Exception as error:
            logger.critical('Unable to initialize the database due to: %s', error)

        # Close the DB connection
        self.close()

    def connect(self, retry_counter=0):
        """
        Connect to the DB
        """
        if not self._connection:
            try:
                self._connection = psycopg2.connect(user=self._username,
                                                    password=self._password,
                                                    host=self._host,
                                                    port=self._port,
                                                    database=self._db)
                retry_counter = 0
                return True
            except psycopg2.OperationalError as error:
                if retry_counter >= DATABASE_CONNECTION_MAX_RETRIES:
                    logger.critical('Unable to connect to the database due to: %s', error)
                else:
                    retry_counter += 1
                    logger.error('Got error "%s" when connecting to the database, retry number: %d', error, retry_counter)
                    time.sleep(2)
                    self.connect(retry_counter)
            except (Exception, psycopg2.Error) as error:
                logger.critical('Unable to connect to the database due to: %s', error)

        if self._connection:
            return True
        return False

    def close(self):
        """
        Close the connection to the DB
        """
        if self._connection:
            self._connection.close()
        self._connection = None

    def reset(self):
        """
        Close and reconnect to the DB
        """
        self.close()
        return self.connect()
    
    def execute(self, query, data=None, retry_counter=0):
        """
        Execute a query
        """
        try:
            cursor = self._connection.cursor()
            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
            self._connection.commit()
            cursor.close()
        except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
            if retry_counter >= DATABASE_QUERY_MAX_RERIES:
                logger.critical('Unable to execute query "%s" due to "%s"', query, error)
                return False
            else:
                retry_counter += 1
                logger.error('Got error "%s" when executing query, retry number: %d', error, retry_counter)
                time.sleep(1)
                self.reset()
                self.execute(query, data, retry_counter=retry_counter)
        except (Exception, psycopg2.Error) as error:
            logger.critical('Unable to execute query "%s" due to "%s"', query, error)
            return False
        return True
