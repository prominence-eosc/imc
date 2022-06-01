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

class AWS():
    """
    AWS EC2 connector
    """
    def __init__(self, info):
        self._ec2 = boto3.Session(aws_access_key_id=info['credentials']['ACCESS_ID'],
                                  aws_secret_access_key=info['credentials']['SECRET_KEY'],
                                  region_name=info['cloudRegion'])

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

    def create_instance(self, name, image, flavor, network, userdata, disk=None):
        """
        Create an instance
        """
        num_cpus = self._get_cpus_from_flavor(flavor)
        args = {'ImageId': image,
                'InstanceType': flavor,
                'MinCount': 1,
                'MaxCount': 1,
                'UserData': userdata,
                'CpuOptions': {'ThreadsPerCore': 1, 'CoreCount': num_cpus},
                'TagSpecifications': [{'ResourceType': 'instance',
                                       'Tags': [{'Key': 'Name',
                                                 'Value': name},
                                                {'Key': 'Creator',
                                                 'Value': 'Prominence'}]}],
                'BlockDeviceMappings': [{'DeviceName': '/dev/sda1',
                                         'Ebs': {'VolumeSize': disk}}],
                'NetworkInterfaces': [{'SubnetId': network,
                                       'DeviceIndex': 0,
                                       'DeleteOnTermination': True}]}

        try:
            instances = self._ec2.resource('ec2').create_instances(**args)
        except Exception as err:
            logger.error('Got exception creating instance: %s', err)
            return None, str(err)

        if instances:
            return instances[0].instance_id, None

        return None, None

    def delete_instance(self, instance_id):
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
            for instance in self._ec2.resource('ec2').instances.filter(Filters=[{'Name': 'tag:Creator',
                                                                                 'Values': ['Prominence']}]):
                data.append({'id': instance.id,
                             'status': instance.state['Name'],
                             'name': get_name(instance.tags)})
        except Exception as err:
            logger.error('Got exception listing instances: %s', err)

        return data

    def get_instance(self, instance_id):
        """
        Get details of the specified instance
        """
        try:
            instance = self._ec2.resource('ec2').Instance(instance_id)
        except Exception as err:
            logger.error('Got exception getting instance: %s', err)
            return None

        return get_name(instance.tags), instance.state['Name']

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
                if not instance_type["InstanceStorageSupported"]:
                    data.append({'cpus': instance_type['VCpuInfo']['DefaultCores'],
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
