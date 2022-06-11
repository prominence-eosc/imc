from .openstack import OpenStack
from .aws import AWS
from .azure import Azure
from .gcp import GCP
from .oracle import Oracle

def Resource(info):
    """
    Factory method for different resources
    """
    resources = {
        "OpenStack": OpenStack,
        "AWS": AWS,
        "Azure": Azure,
        "GCP": GCP,
        "OCI": Oracle
    }

    return resources[info['resource_type']](info)
