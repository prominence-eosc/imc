"""Miscellaneous functions"""

from __future__ import print_function
import logging
import re

from imc import config

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

def valid_uuid(uuid):
    """
    Check if the given string is a valid uuid
    """
    regex = re.compile('^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z', re.I)
    match = regex.match(uuid)
    return bool(match)

def get_reqs_and_prefs(description):
    """
    Extract the requirements & preferences from the input JSON description
    """
    if 'preferences' in description:
        preferences_new = {}
        # Generate list of weighted regions if necessary
        if 'regions' in description['preferences']:
            preferences_new['regions'] = {}
            for i in range(0, len(description['preferences']['regions'])):
                preferences_new['regions'][description['preferences']['regions'][i]] = len(description['preferences']['regions']) - i
        # Generate list of weighted sites if necessary
        if 'sites' in description['preferences']:
            preferences_new['sites'] = {}
            for i in range(0, len(description['preferences']['sites'])):
                preferences_new['sites'][description['preferences']['sites'][i]] = len(description['preferences']['sites']) - i
        description['preferences'] = preferences_new
    else:
        preferences = {}

    if 'requirements' in description:
        requirements = description['requirements']
        preferences = description['preferences']
    else:
        requirements = {}
    
    return (requirements, preferences)
