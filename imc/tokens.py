"""Functions for handling tokens"""

import json
import logging
import os
import time
import requests

try:
    from urlparse import urlparse, urlunparse
except ImportError:
    from urllib.parse import urlparse, urlunparse

import timeout_decorator

from imc import config
from imc import utilities

# Configuration
CONFIG = config.get_config()
CLOUD_TIMEOUT = int(CONFIG.get('timeouts', 'cloud'))

# Logging
logger = logging.getLogger(__name__)

def get_token(cloud, identity, db, config):
    """
    Get a token for a cloud
    """
    logger.info('Checking if we need a token for cloud %s', cloud)

    # Get config for the required cloud
    data = {}
    for cloud_info in config:
        if cloud_info['name'] == cloud:
            data = cloud_info

    if not data:
        logger.critical('Unable to find info for cloud %s in JSON config', cloud)
        return None

    # Get details required for generating a new token
    (user_token, client_id, client_secret, refresh_token, scope, url) = check_if_token_required(cloud, data)
    if not client_id or not client_secret or not scope or not url:
        logger.info('A token is not required for cloud %s', cloud)
        return None

    if user_token:
        logger.info('Cloud %s requires user tokens', cloud)

    # Try to obtain an existing token from the DB
    logger.info('Try to get an existing token from the DB')
    if not user_token:
        (token, expiry, creation) = db.get_token(cloud)
    else:
        (refresh_token, token, creation, expiry) = db.get_user_credentials(identity)

    # Check token
    if token:
        check_rt = check_token(token, url)
        if check_rt != 0:
            logger.info('Check token failed for cloud %s', cloud)
    else:
        logger.info('No token could be obtained from the DB for cloud %s', cloud)
        check_rt = -1

    logger.info('Token expiry time: %d, current time: %d', expiry, time.time())
    if expiry - time.time() < 600:
        logger.info('Token has or is about to expire')

    if not token or expiry - time.time() < 600 or (check_rt != 0 and time.time() - creation > 600):
        logger.info('Getting a new token for cloud %s', cloud)
        # Get new token
        (token, expiry, creation, _) = get_new_token(client_id, client_secret, refresh_token, scope, url)

        if not token:
            logger.critical('Unable to get a new access token')

        # Update token in DB
        if user_token:
            db.update_user_access_token(identity, token, expiry, creation)
        else:
            db.delete_token(cloud)
            db.set_token(cloud, token, expiry, creation)

    else:
        logger.info('Using token from DB for cloud %s', cloud)

    return token

def get_new_token(client_id, client_secret, refresh_token, scope, url):
    """
    Get a new access token using a refresh token
    """
    creation = time.time()
    data = {'client_id':client_id,
            'client_secret':client_secret,
            'grant_type':'refresh_token',
            'refresh_token':refresh_token,
            'scope':scope}
    try:
        response = requests.post(url + '/token', auth=(client_id, client_secret), timeout=10, data=data)
    except requests.exceptions.Timeout:
        return (None, 0, 0, 'timed out')
    except requests.exceptions.RequestException as ex:
        return (None, 0, 0, ex)

    if response.status_code == 200:
        access_token = response.json()['access_token']
        expires_at = int(response.json()['expires_in'] + creation)
        return (access_token, expires_at, creation, '')
    return (None, 0, 0, response.text)

def check_token(token, url):
    """
    Check whether a token is valid
    """
    header = {"Authorization":"Bearer %s" % token}

    try:
        response = requests.get(url + '/userinfo', headers=header, timeout=10)
    except requests.exceptions.Timeout:
        return 2
    except requests.exceptions.RequestException:
        return 2

    if response.status_code == 200:
        return 0
    return 1

def check_if_token_required(cloud, data):
    """
    Check if the given cloud requires a token for access
    """
    if 'credentials' in data:
        if 'token' in data['credentials']:
            user_token = False
            refresh_token = None
            if 'provider' in data['credentials']['token']:
                if data['credentials']['token']['provider'] == 'user':
                    user_token = True
            if 'client_id' not in data['credentials']['token']:
                logger.error('client_id not defined in token section of credentials for cloud %s', cloud)
                return None
            if 'client_secret' not in data['credentials']['token']:
                logger.error('client_secret not defined in token section of credentials for cloud %s', cloud)
                return None
            if 'refresh_token' in data['credentials']['token']:
                refresh_token = data['credentials']['token']['refresh_token']
            if 'scope' not in data['credentials']['token']:
                logger.error('scope not defined in token section of credentials for cloud %s', cloud)
                return None
            if 'url' not in data['credentials']['token']:
                logger.error('url not defined in token section of credentials for cloud %s', cloud)
                return None

            return (user_token,
                    data['credentials']['token']['client_id'],
                    data['credentials']['token']['client_secret'],
                    refresh_token,
                    data['credentials']['token']['scope'],
                    data['credentials']['token']['url'])

    return (None, None, None, None, None, None)

def get_keystone_url(os_auth_url, path):
    """
    Generate keystone URL
    """
    url = urlparse(os_auth_url)
    prefix = url.path.rstrip('/')
    if prefix.endswith('v2.0') or prefix.endswith('v3'):
        prefix = os.path.dirname(prefix)
    path = os.path.join(prefix, path)
    return urlunparse((url[0], url[1], path, url[3], url[4], url[5]))

def get_unscoped_token(os_auth_url, access_token, username, tenant_name):
    """
    Get an unscoped token from an access token
    """
    url = get_keystone_url(os_auth_url,
                           '/v3/OS-FEDERATION/identity_providers/%s/protocols/%s/auth' % (username, tenant_name))
    response = requests.post(url,
                             headers={'Authorization': 'Bearer %s' % access_token})

    if 'X-Subject-Token' in response.headers:
        return response.headers['X-Subject-Token']
    return None

@timeout_decorator.timeout(CLOUD_TIMEOUT)
def get_scoped_token(os_auth_url, os_project_id, unscoped_token):
    """
    Get a scoped token from an unscoped token
    """
    url = get_keystone_url(os_auth_url, '/v3/auth/tokens')
    token_body = {
        "auth": {
            "identity": {
                "methods": ["token"],
                "token": {"id": unscoped_token}
            },
            "scope": {"project": {"id": os_project_id}}
        }
    }
    response = requests.post(url, headers={'content-type': 'application/json'},
                             data=json.dumps(token_body))

    if 'X-Subject-Token' in response.headers:
        return response.headers['X-Subject-Token']
    return None
