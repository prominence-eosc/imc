import base64
import logging

import oci

# Logging
logger = logging.getLogger(__name__)

def status_map(status):
    """
    Map OCI status
    """
    if status == 'RUNNING':
        return 'running'
    elif status in ('PROVISIONING', 'STARTING', 'SCALING'):
        return 'pending'
    elif status in ('SUSPENDING' 'SUSPENDED'):
        return 'stopped'
    elif status in ('STOPPING', 'TERMINATING', 'TERMINATED'):
        return 'terminated'
    else:
        logger.error('OCI status is %s, returning unknown', status)
        return 'unknown'

class Oracle():
    """
    OCI connector
    """
    def __init__(self, info):
        config = info['credentials']

        try:
            from oci.config import validate_config
            validate_config(config)
        except Exception as err:
            logger.error('Error checking supplied config: %s', err)

        self._identity_client = oci.identity.IdentityClient(config)
        self._compute_client = oci.core.ComputeClient(config)
        self._info = info

    def create_instance(self, name, image, flavor, network, security_groups, userdata, disk, infra_id, unique_infra_id):
        """
        Create an instance
        """
        # Boot volumes must be 50 GB or more in size
        if disk < 50:
            disk = 50

        launch_instance_details = oci.core.models.LaunchInstanceDetails(
            compartment_id=self._info['compartment_id'],
            availability_domain=self._info['availability_domain_name'],
            display_name=name,
            freeform_tags={'creator': 'prominence',
                           'prominence-infra-id': infra_id,
                           'prominence-unique-infra-id': unique_infra_id},
            metadata={'user_data': base64.b64encode(userdata.encode('ascii')).decode('utf-8')},
            shape=flavor,
            create_vnic_details=oci.core.models.CreateVnicDetails(subnet_id=network),
            source_details=oci.core.models.InstanceSourceViaImageDetails(image_id=image,
                                                                         boot_volume_size_in_gbs=disk)
        )

        try:
            response = self._compute_client.launch_instance(launch_instance_details)
        except Exception as err:
            logger.error('Got exception creating instance: %s', err)
            return None, str(err)

        if response.data:
            return response.data.id, None

        return None, None

    def delete_instance(self, instance_name, instance_id):
        """
        Delete the specified instance
        """
        try:
            self._compute_client.terminate_instance(instance_id)
        except Exception as err:
            logger.error('Got exception deleting instance: %s', err)
            return False

        return True

    def list_instances(self):
        """
        List instances
        """
        try:
            response = oci.pagination.list_call_get_all_results(self._compute_client.list_instances,
                                                                compartment_id=self._info['compartment_id'])
        except Exception as err:
            logger.error('Got exception listing instances: %s', err)
            return None

        data = []
        for instance in response.data:
            if len(instance.freeform_tags) > 0:
                if 'creator' in instance.freeform_tags:
                    if instance.freeform_tags['creator'] == 'prominence':
                        data.append({'id': instance.id,
                                     'name': instance.display_name,
                                     'status': status_map(instance.lifecycle_state),
                                     'metadata': instance.freeform_tags})
        return data

    def get_instance(self, instance_name, instance_id):
        """
        Get details of the specified instance
        """
        try:
            instance = self._compute_client.get_instance(instance_id)
        except Exception as err:
            if 'NotAuthorizedOrNotFound' in str(err):
                return False

            logger.error('Got exception getting instance: %s', err)
            return None

        if instance.data:
            return status_map(instance.data.lifecycle_state)

        return None

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
        try:
            response = oci.pagination.list_call_get_all_results(self._compute_client.list_shapes,
                                                                self._info['compartment_id'],
                                                                availability_domain=self._info['availability_domain_name'])
            shapes = response.data
        except Exception as err:
            logger.info('Got exception listing flavours: %s', err)
            return None

        vm_shapes = list(filter(lambda shape: shape.shape.startswith("VM"), shapes))
        data = []
        for vm in vm_shapes:
            data.append({'id': vm.shape,
                         'name': vm.shape,
                         'cpus': vm.ocpus,
                         'memory': vm.memory_in_gbs,
                         'disk': -1})
        return data

    def get_quotas(self):
        """
        Get quotas
        """
        quotas = {}
        return quotas
