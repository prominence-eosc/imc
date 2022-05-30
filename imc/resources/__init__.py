from .openstack import OpenStack

def Resource(resource_type, *kwargs):
    """
    Factory method for different resources
    """
    resources = {
        "openstack": OpenStack
    }

    return resources[resource_type](*kwargs)
