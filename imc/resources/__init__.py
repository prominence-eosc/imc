from .openstack import OpenStack
from .aws import AWS
from .gcp import GCP
from .oracle import Oracle

def Resource(info):
    """
    Factory method for different resources
    """
    resources = {
        "OpenStack": OpenStack,
        "AWS": AWS,
        "GCP": GCP,
        "OCI": Oracle
    }

    return resources[info['resource_type']](info)
