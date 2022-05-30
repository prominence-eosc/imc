import logging

from novaclient import client
from glanceclient import Client
from keystoneauth1 import loading, session

# Logging
logger = logging.getLogger(__name__)

class OpenStack():
    """
    OpenStack connector
    """
    def __init__(self, credentials, credential_type='password', region=None):
        loader = loading.get_plugin_loader(credential_type)
        auth = loader.load_from_options(**credentials)
        self._session = session.Session(auth=auth)
        self._credentials = credentials

    def create_instance(self, name, image, flavor, network, userdata):
        """
        Create an instance
        """
        try:
            nova = client.Client(2, session=self._session)
            server = nova.servers.create(name,
                                         image=image,
                                         flavor=flavor,
                                         nics=[{'net-id': network}],
                                         userdata=userdata).to_dict()
        except Exception as err:
            logger.error('Got exception creating server: %s', err)
            return None

        return server['id']

    def delete_instance(self, instance_id):
        """
        Delete the specified instance
        """
        try:
            nova = client.Client(2, session=self._session)
            nova.servers.delete(instance_id)
        except Exception as err:
            logger.error('Got exception deleting server: %s', err)
            return False

        return True

    def list_instances(self):
        """
        List instances
        """
        data = []
        try:
            nova = client.Client(2, session=self._session)
            results = nova.servers.list()
            for server in results:
                server_dict = server.to_dict()
                if server_dict['name'].startswith('prominence-'):
                    data.append({'id': server_dict['id'],
                                 'name': server_dict['name'],
                                 'status': server_dict['status']})
        except Exception as err:
            logger.error('Got exception listing instances: %s', err)

        return data

    def get_instance(self, instance_id):
        """
        Get details of the specified instance
        """
        try:
            nova = client.Client(2, session=self._session)
            result = nova.servers.get(instance_id).to_dict()
        except Exception as err:
            logger.error('Got exception getting instance: %s', err)
            return None

        return {'name': result['name'],
                'status': result['status']}

    def list_images(self):
        """
        Get images
        """
        data = []
        try:
            glance = Client('2', session=self._session)
            results = glance.images.list()
        except Exception as err:
            logger.error('Got exception listing images: %s', err)
            return None

        for item in results:
            data.append({item['id'], item['name']})

        return data

    def list_flavors(self):
        """
        Get flavours
        """
        data = []
        try:
            nova = client.Client(2, session=self._session)
            results = nova.flavors.list()
            for flavor in results:
                flavor_dict = flavor.to_dict()
                data.append({'id': flavor_dict['id'],
                             'name': flavor_dict['name'],
                             'cpus': flavor_dict['vcpus'],
                             'memory': flavor_dict['ram'],
                             'disk': flavor_dict['disk']})
        except Exception as err:
            logger.error('Got exception listing flavours: %s', err)

        return data

    def get_quotas(self):
        """
        Get quotas
        """
        # First try to get limits only, as some OpenStack clouds do not allow normal users to get usage
        quotas = {}
        try:
            nova = client.Client(2, session=self._session)
            os_quotas = nova.quotas.get(self._credentials['project_id'], detail=False).to_dict()
        except Exception as err:
            logger.error('Got exception getting quotas (limits only): %s', err)
            return None

        quotas['limits'] = {}
        quotas['limits']['cpus'] = os_quotas['cores']
        quotas['limits']['memory'] = int(os_quotas['ram']/1024)
        quotas['limits']['instances'] = os_quotas['instances']

        # Now try to get usage
        try:
            nova = client.Client(2, session=self._session)
            os_quotas = nova.quotas.get(self._credentials['project_id'], detail=True).to_dict()
        except Exception as err:
            logger.error('Got exception getting quotas (usage): %s', err)
            return quotas

        quotas['usage'] = {}
        quotas['usage']['cpus'] = os_quotas['cores']['in_use'] + os_quotas['cores']['reserved']
        quotas['usage']['memory'] = int((os_quotas['ram']['in_use'] + os_quotas['ram']['reserved'])/1024)
        quotas['usage']['instances'] = os_quotas['instances']['in_use'] + os_quotas['instances']['reserved']

        return quotas