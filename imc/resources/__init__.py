from .openstack import OpenStack
from .aws import AWS

def Resource(info):
    """
    Factory method for different resources
    """
    resources = {
        "OpenStack": OpenStack,
        "AWS": AWS
    }

    return resources[info['resource_type']](info)
