import logging

from google.cloud import compute_v1
from google.oauth2 import service_account

# Logging
logger = logging.getLogger(__name__)

def status_map(status):
    """
    Map GCP status
    """
    if status == 'RUNNING':
        return 'running'
    elif status in ('PROVISIONING', 'STAGING'):
        return 'pending'
    elif status in ('SUSPENDING' 'SUSPENDED'):
        return 'stopped'
    elif status in ('STOPPING', 'TERMINATED'):
        return 'terminated'
    elif status == 'REPAIRING':
        return 'error'
    else:
        logger.error('GCP status is %s, returning unknown', status)
        return 'unknown'

class GCP():
    """
    GCP connector
    """
    def __init__(self, info):
        self._credentials = service_account.Credentials.from_service_account_info(info['credentials'])
        self._info = info

    def create_instance(self, name, image, flavor, network, userdata, disk, infra_id, unique_infra_id):
        """
        Create an instance
        """
        # Disk
        boot_disk = compute_v1.AttachedDisk()
        initialize_params = compute_v1.AttachedDiskInitializeParams()
        initialize_params.source_image = image
        initialize_params.disk_size_gb = disk
        initialize_params.disk_type = 'projects/%s/zones/%s/diskTypes/pd-balanced' % (self._info['credentials']['project_id'],
                                                                                      self._info['cloud_region'])        
        boot_disk.initialize_params = initialize_params
        boot_disk.auto_delete = True
        boot_disk.boot = True

        # Userdata
        items = compute_v1.Items()
        items.key = 'startup-script'
        items.value = userdata
        metadata = compute_v1.Metadata()
        metadata.items = [items]

        # Instance
        instance = compute_v1.Instance()
        instance.name = name
        instance.labels = {'prominence-infra-id': infra_id,
                           'prominence-unique-infra-id': unique_infra_id,
                           'creator': 'prominence'}
        instance.machine_type = 'zones/%s/machineTypes/%s' % (self._info['cloud_region'], flavor)
        instance.disks = [boot_disk]

        if self._info['disable_hyperthreading']:
            advanced_machine_features = compute_v1.AdvancedMachineFeatures()
            advanced_machine_features.threads_per_core = 1
            instance.advanced_machine_features = advanced_machine_features

        network_interface = compute_v1.NetworkInterface()
        network_interface.name = 'global/networks/default'
        instance.network_interfaces = [network_interface]
        instance.metadata = metadata

        instance_client = compute_v1.InstancesClient(credentials=self._credentials)
        request = compute_v1.InsertInstanceRequest()
        request.project = self._info['credentials']['project_id']
        request.zone = self._info['cloud_region']
        request.instance_resource = instance

        try:
            instance_client.insert(request=request)
        except Exception as err:
            logger.error('Got exception creating instance: %s', err)
            return None, str(err)

        return name, None

    def delete_instance(self, instance_name, instance_id):
        """
        Delete the specified instance
        """
        instance_client = compute_v1.InstancesClient(credentials=self._credentials)
        try:
            instance_client.delete(project=self._info['credentials']['project_id'],
                                   zone=self._info['cloud_region'],
                                   instance=instance_name)
        except Exception as err:
            logger.info('Got exception deleting instance: %s', err)
            return False

        return True

    def list_instances(self):
        """
        List instances
        """
        instance_client = compute_v1.InstancesClient(credentials=self._credentials)
        request = compute_v1.ListInstancesRequest()
        request.project = self._info['credentials']['project_id']
        request.zone = self._info['cloud_region']
        request.filter = 'labels.creator=prominence'
        
        data = []
        try:
            instances = instance_client.list(request=request)
        except Exception as err:
            logger.error('Got exception listing instances: %s', err)
            return None

        for instance in instances:
            data.append({'id': instance.id,
                         'name': instance.name,
                         'status': status_map(instance.status),
                         'metadata': instance.labels})

        return data

    def get_instance(self, instance_name, instance_id):
        """
        Get details of the specified instance
        """
        instance_client = compute_v1.InstancesClient(credentials=self._credentials)
        request = compute_v1.GetInstanceRequest()
        request.project = self._info['credentials']['project_id']
        request.zone = self._info['cloud_region']
        request.instance = instance_name

        try:
            instance = instance_client.get(request=request)
        except Exception as err:
            logger.error('Got exception getting instance details: %s', err)
            return None

        return status_map(instance.status)

    def list_images(self):
        """
        Get images
        """
        data = []
        return data

    def list_flavors(self):
        """
        Get flavours
        """
        machine_client = compute_v1.MachineTypesClient(credentials=self._credentials)
        request = compute_v1.ListMachineTypesRequest()
        request.project = self._info['credentials']['project_id']
        request.zone = self._info['cloud_region']

        data = []
        try:
            machine_types = machine_client.list(request=request)
        except Exception as err:
            logger.error('Got exception listing machine types: %s', err)
            return None

        for machine_type in machine_types:
            if self._info['disable_hyperthreading']:
                # See https://cloud.google.com/compute/docs/machine-types
                threads_per_core = 2
                if machine_type.name.startswith('t2d'):
                    threads_per_core = 1
            else:
                threads_per_core = 1

            data.append({'id': machine_type.name, # need to use names when creating instances, not id
                         'name': machine_type.name,
                         'cpus': machine_type.guest_cpus/threads_per_core,
                         'memory': machine_type.memory_mb/1024,
                         'disk': -1})

        return data

    def get_quotas(self):
        """
        Get quotas
        """
        quotas = {}
        return quotas
