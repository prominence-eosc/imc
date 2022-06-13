from enum import Enum
class DeploymentStatus(Enum):
    SUCCESS = 0
    FAILED = 1
    NOT_YET_RUNNING = 2
    WAITING_TOO_LONG = 5
    QUOTA_EXCEEDED = 6
    IMAGE_ERROR = 7
    FLAVOUR_ERROR = 8
    INSUFFICIENT_CAPACITY = 9
