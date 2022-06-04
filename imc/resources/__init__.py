from .openstack import OpenStack
from .aws import AWS
from .gcp import GCP

def Resource(info):
    """
    Factory method for different resources
    """
    resources = {
        "OpenStack": OpenStack,
        "AWS": AWS,
        "GCP": GCP
    }

    return resources[info['resource_type']](info)
