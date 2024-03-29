import logging

# Logging
logger = logging.getLogger(__name__)

def set_egi_cloud(self, identity, name, auth_url, project_id, project_domain_id, user_domain_name, region, protocol):
    """
    Create/update the entry for the specified cloud
    """
    try:
        cursor = self._connection.cursor()
        cursor.execute("UPDATE egi_clouds SET auth_url='%s',project_id='%s',project_domain_id='%s',user_domain_name='%s',region='%s',protocol='%s' WHERE identity='%s' AND site='%s'" % (auth_url, project_id, project_domain_id, user_domain_name, region, protocol, identity, name))
        cursor.execute("INSERT INTO egi_clouds (identity, site, auth_url, project_id, project_domain_id, user_domain_name, region, protocol) SELECT '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' WHERE NOT EXISTS (SELECT 1 FROM egi_clouds WHERE identity='%s' AND site='%s')" % (identity, name, auth_url, project_id, project_domain_id, user_domain_name, region, protocol, identity, name))
        self._connection.commit()
        cursor.close()
    except Exception as error:
        logger.critical('[set_egi_cloud] Unable to execute UPDATE or INSERT query due to: %s', error)
        return False
    return True

def get_egi_clouds(self, identity):
    """
    Return all EGI Federated Clouds for the specified identity
    """
    clouds = {}
    try:
        cursor = self._connection.cursor()
        cursor.execute("SELECT site, auth_url, project_id, project_domain_id, user_domain_name, region, protocol FROM egi_clouds WHERE identity='%s' AND enabled='true'" % identity)
        for row in cursor:
            cloud = {}
            cloud['name'] = row[0]
            cloud['credentials'] = {}
            cloud['credentials']['host'] = row[1]
            cloud['credentials']['project_id'] = row[2]
            cloud['credentials']['tenant_id'] = row[2]
            cloud['credentials']['service_region'] = row[5]
            cloud['credentials']['project_domain_id'] = row[3]
            cloud['credentials']['user_domain_name'] = row[4]
            cloud['credentials']['tenant'] = row[6]
            cloud['credentials']['type'] = 'OpenStack'
            clouds[cloud['name']] = cloud
        cursor.close()
    except Exception as error:
        logger.critical('[get_egi_clouds] Unable to execute SELECT query due to: %s', error)

    return clouds

def disable_egi_clouds(self, identity, clouds):
    """
    Disable all clouds, if any, except for those specified
    """
    clouds_str = ','.join(["'{}'".format(value) for value in clouds])

    return self.execute("UPDATE egi_clouds SET enabled='false' WHERE identity='%s' AND site NOT IN (%s)" % (identity, clouds_str))
