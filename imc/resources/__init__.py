from .openstack import OpenStack
from .aws import AWS

def Resource(resource_type, *kwargs):
    """
    Factory method for different resources
    """
    resources = {
        "openstack": OpenStack,
        "aws": AWS
    }

    return resources[resource_type](*kwargs)
