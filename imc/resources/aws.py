import logging

import boto3

# Logging
logger = logging.getLogger(__name__)

def get_name(tags):
    """
    Get instance name from tags
    """
    for pair in tags:
        if pair['Key'] == 'Name':
            return pair['Value']
    return None

def to_dict(tags):
    """
    Convert to dictionary
    """
    dict = {}
    for pair in tags:
        dict[pair['Key']] = pair['Value']
    return dict

class AWS():
    """
    AWS EC2 connector
    """
    def __init__(self, info):
        self._ec2 = boto3.Session(aws_access_key_id=info['credentials']['ACCESS_ID'],
                                  aws_secret_access_key=info['credentials']['SECRET_KEY'],
                                  region_name=info['cloud_region'])
        self._info = info

    def _get_cpus_from_flavor(self, flavor):
        """
        Return the number of cores in the specified flavor
        """
        try:
            instance_types = self._ec2.client('ec2').describe_instance_types(InstanceTypes=[flavor])['InstanceTypes']
        except Exception as err:
            logger.error('Got exception getting flavor details: %s', err)
            return None

        if instance_types:
            return instance_types[0]['VCpuInfo']['DefaultCores']

        return None

    def create_instance(self, name, image, flavor, network, security_groups, userdata, disk, infra_id, unique_infra_id):
        """
        Create an instance
        """
        args = {'ImageId': image,
                'InstanceType': flavor,
                'MinCount': 1,
                'MaxCount': 1,
                'UserData': userdata,
                'TagSpecifications': [{'ResourceType': 'instance',
                                       'Tags': [{'Key': 'Name',
                                                 'Value': name},
                                                {'Key': 'creator',
                                                 'Value': 'prominence'},
                                                {'Key': 'prominence-infra-id',
                                                 'Value': infra_id},
                                                {'Key': 'prominence-unique-infra-id',
                                                 'Value': unique_infra_id}]}],
                'BlockDeviceMappings': [{'DeviceName': '/dev/sda1',
                                         'Ebs': {'VolumeSize': disk}}],
                'NetworkInterfaces': [{'SubnetId': network,
                                       'DeviceIndex': 0,
                                       'DeleteOnTermination': True}]}

        if self._info['disable_hyperthreading']:
            num_cpus = self._get_cpus_from_flavor(flavor)
            args['CpuOptions'] = {'ThreadsPerCore': 1, 'CoreCount': num_cpus}

        try:
            instances = self._ec2.resource('ec2').create_instances(**args)
        except Exception as err:
            logger.error('Got exception creating instance: %s', err)
            return None, str(err)

        if instances:
            return instances[0].instance_id, None

        return None, None

    def delete_instance(self, instance_name, instance_id):
        """
        Delete the specified instance
        """
        try:
            self._ec2.resource('ec2').Instance(instance_id).terminate()
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
            for instance in self._ec2.resource('ec2').instances.filter(Filters=[{'Name': 'tag:creator',
                                                                                 'Values': ['prominence']}]):
                data.append({'id': instance.id,
                             'status': instance.state['Name'],
                             'name': get_name(instance.tags),
                             'metadata': to_dict(instance.tags)})
        except Exception as err:
            logger.error('Got exception listing instances: %s', err)

        return data

    def get_instance(self, instance_name, instance_id):
        """
        Get details of the specified instance
        """
        state = None
        try:
            instance = self._ec2.resource('ec2').Instance(instance_id)
            state = instance.state['Name']
        except Exception as err:
            if 'InvalidInstanceID.NotFound' in str(err):
                state = False
            logger.error('Got exception getting instance: %s', err)

        return state

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
        data = []
        try:
            for instance_type in self._ec2.client('ec2').describe_instance_types()['InstanceTypes']:
                if instance_type["InstanceStorageSupported"]:
                    continue

                if self._info['disable_hyperthreading']:
                    threads_per_core = instance_type['VCpuInfo']['DefaultThreadsPerCore']
                else:
                    threads_per_core = 1

                data.append({'cpus': instance_type['VCpuInfo']['DefaultCores']/threads_per_core,

                             'memory': int(instance_type['MemoryInfo']['SizeInMiB']/1024),
                             'disk': -1,
                             'name': instance_type['InstanceType'],
                             'id': instance_type['InstanceType']})
        except Exception as err:
            logger.error('Got exception listing flavours: %s', err)

        return data

    def get_quotas(self):
        """
        Get quotas
        """
        quotas = {}
        return quotas
