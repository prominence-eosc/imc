"""Run pilot job on the specified batch system, with extensive error handling"""

from __future__ import print_function
import os
import sys
from string import Template
import time
import random
import logging
import configparser

from imc import config
from imc import database
from imc import destroy
from imc import opa_client
from imc import tokens

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def batch_deploy(radl, resource_name, time_begin, unique_id, identity, db, num_nodes=1):
    """
    Submit a startd job to the specified batch system
    """
