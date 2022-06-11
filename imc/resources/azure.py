import logging
import string
import random
import os

from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient

# Logging
logger = logging.getLogger(__name__)

def generate_password(length):
    """
    Generate random password
    """
    characters = list(string.ascii_letters + string.digits + "!@#$%^&*()")
    random.shuffle(characters)
    password = []
    for i in range(length):
        password.append(random.choice(characters))
    random.shuffle(password)
    return "".join(password)

class Azure():
    """
    Azure connector
    """
    def __init__(self, info):
        os.environ['AZURE_CLIENT_ID'] = info['credentials']['client_id']
        os.environ['AZURE_CLIENT_SECRET'] = info['credentials']['client_secret']
        os.environ['AZURE_TENANT_ID'] = info['credentials']['tenant_id']
        self._credential = DefaultAzureCredential()
        self._info = info

    def create_instance(self, name, image, flavor, network, security_groups, userdata, disk, infra_id, unique_infra_id):
        """
        Create an instance
        """
        compute_client = ComputeManagementClient(credential=self._credential,
                                                 subscription_id=self._info['credentials']['subscription_id'])
        network_client = NetworkManagementClient(credential=self._credential,
                                                 subscription_id=self._info['credentials']['subscription_id'])
        # Provision the NIC
        logger.info('Provisioning the NIC...')
        nic_data = {
            "location": self._info['cloud_region'],
            "ip_configurations": [ {
                "name": "prominence-ip-%s" % unique_infra_id,
                "subnet": { "id": network }
            }]
        }
        try:
            poller = network_client.network_interfaces.begin_create_or_update(self._info['credentials']['resource_group'],
                                                                              "prominence-nic-%s" % unique_infra_id,
                                                                              nic_data)
            nic_result = poller.result()
        except Exception as err:
            return None, str(err)

        # Provision the VM
        logger.info('Provisioning the instance...')
        instance_data = {
            "location": self._info['cloud_region'],
            "tags": {
                "Creator": "Prominence",
                "ProminenceInfrastructureId": infra_id,
                "ProminenceUniqueInfrastructureId": unique_infra_id,
            },
            "storage_profile": {
                "image_reference": {
                    "publisher": image.split('/')[0],
                    "offer": image.split('/')[1],
                    "sku": image.split('/')[2],
                    "version": image.split('/')[3]
                }
            },
            "hardware_profile": {
                "vm_size": flavor
            },
            "os_profile": {
                "computer_name": name,
                "admin_username": "admin",
                "admin_password": generate_password(12)
            },
            "network_profile": {
                "network_interfaces": [{
                    "id": nic_result.id,
                }]
            },
            "userData": userdata
        }

        try:
            poller = compute_client.virtual_machines.begin_create_or_update(self._info['credentials']['resource_group'],
                                                                            name,
                                                                            instance_data)
            instance_result = poller.result()
        except Exception as err:
            return None, str(err)

        return instance_result.id, None

    def delete_instance(self, instance_name, instance_id):
        """
        Delete the specified instance
        """
        compute_client = ComputeManagementClient(credential=self._credential,
                                                 subscription_id=self._info['credentials']['subscription_id'])
        network_client = NetworkManagementClient(credential=self._credential,
                                                 subscription_id=self._info['credentials']['subscription_id'])

        try:
            logger.info('Deleting instance...')
            async_vm_delete = compute_client.virtual_machines.begin_delete(self._info['credentials']['resource_group'],
                                                                           instance_name)
            async_vm_delete.wait()
            logger.info('Deleting network interface...')
            net_del_poller = network_client.network_interfaces.begin_delete(self._info['credentials']['resource_group'],
                                                                            instance_name.replace('prominence',
                                                                                                  'prominence-nic'))
            net_del_poller.wait()
            logger.info('Deleting disks...')
            disks_list = compute_client.disks.list_by_resource_group(self._info['credentials']['resource_group'])
            async_disk_handle_list = []
            for disk in disks_list:
                if instance_name in disk.name:
                    async_disk_delete = compute_client.disks.begin_delete(self._info['credentials']['resource_group'],
                                                                          disk.name)
                    async_disk_handle_list.append(async_disk_delete)
            for async_disk_delete in async_disk_handle_list:
                async_disk_delete.wait()
        except Exception as err:
            logger.error('Got exception deleting instance: %s', err)
            return None

        return True

    def list_instances(self):
        """
        List instances
        """
        client = ComputeManagementClient(credential=self._credential,
                                         subscription_id=self._info['credentials']['subscription_id'])

        data = []
        try:
            for instance in client.virtual_machines.list_all():
                if 'Creator' in instance.tags:
                    if instance.tags['Creator'] == 'Prominence':
                        data.append({'id': instance.id,
                                     'status': instance.provisioning_state,
                                     'name': instance.name,
                                     'metadata': instance.tags})
        except Exception as err:
            logger.error('Got exception listing instances: %s', err)

        return data

    def get_instance(self, instance_name, instance_id):
        """
        Get details of the specified instance
        """
        client = ComputeManagementClient(credential=self._credential,
                                         subscription_id=self._info['credentials']['subscription_id'])

        try:
            instance = client.virtual_machines.get(client, instance_name)
            state = instance.provisioning_state
        except Exception as err:
            if 'azure.core.exceptions.ResourceNotFoundError' in str(err):
                return False
            logger.error('Got exception getting instance: %s', err)
            return None

        return instance.provisioning_state

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
        client = ComputeManagementClient(credential=self._credential,
                                         subscription_id=self._info['credentials']['subscription_id'])
        data = []
        try:
            for vm_size in client.virtual_machine_sizes.list(location=self._info['cloud_region']):
                data.append({'id': vm_size.name,
                             'name': vm_size.name,
                             'cpus': vm_size.number_of_cores,
                             'memory': int(vm_size.memory_in_mb/1024),
                             'disk': int(vm_size.resource_disk_size_in_mb/1024)})
        except Exception as err:
            logger.error('Got exception getting flavours: %s', err)
            return None

        return data

    def get_quotas(self):
        """
        Get quotas
        """
        quotas = {}
        return quotas
