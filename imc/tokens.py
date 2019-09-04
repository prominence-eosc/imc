"""Functions for handling tokens"""

import json
import logging
import os
import sys
import time
import requests

try:
    from urlparse import urlparse, urlunparse, urljoin
except ImportError:
    from urllib.parse import urlparse, urlunparse, urljoin

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

def get_token(cloud, db, config_file):
    """
    Get a token for a cloud
    """
    logger.info('Checking if we need a token for cloud %s', cloud)

    # Get details required for generating a new token
    (username, password, client_id, client_secret, refresh_token, scope, url) = check_if_token_required(cloud, config_file)
    if username is None or password is None or client_id is None or client_secret is None or refresh_token is None or scope is None or url is None:
        logger.info('A token is not required for cloud %s', cloud)
        return None

    # Try to obtain an existing token from the DB
    logger.info('Try to get an existing token from the DB')
    (token, expiry, creation) = db.get_token(cloud)

    # Check token
    if token is not None:
        check_rt = check_token(token, url)
        if check_rt != 0:
            logger.info('Check token failed for cloud %s', cloud)
    else:
        logger.info('No token could be obtained from the DB for cloud %s', cloud)
        check_rt = -1

    logger.info('Token expiry time: %d, current time: %d', expiry, time.time())
    if expiry - time.time() < 600:
        logger.info('Token has or is about to expire')

    if token is None or expiry - time.time() < 600 or (check_rt != 0 and time.time() - creation > 600):
        logger.info('Getting a new token for cloud %s', cloud)
        # Get new token
        (token, expiry, creation, msg) = get_new_token(username, password, client_id, client_secret, refresh_token, scope, url)

        # Delete existing token from DB
        db.delete_token(cloud)

        # Save token to DB
        db.set_token(cloud, token, expiry, creation)
    else:
        logger.info('Using token from DB for cloud %s', cloud)

    return token

def get_new_token(username, password, client_id, client_secret, refresh_token, scope, url):
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
        response = requests.post(url + '/token', auth=(username, password), timeout=10, data=data)
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

def check_if_token_required(cloud, config_file):
    """
    Check if the given cloud requires a token for access
    """
    try:
        with open(config_file) as file:
            data = json.load(file)
    except Exception as ex:
        logger.critical('Unable to open file containing tokens due to: %s', ex)
        return (None, None, None, None, None, None, None)

    if 'credentials' in data:
        if cloud in data['credentials']:
            if 'token' in data['credentials'][cloud]:
                if 'username' not in data['credentials'][cloud]['token']:
                    logger.error('username not defined in token section of credentials for cloud %s', cloud)
                    return None
                if 'password' not in data['credentials'][cloud]['token']:
                    logger.error('password not defined in token section of credentials for cloud %s', cloud)
                    return None
                if 'client_id' not in data['credentials'][cloud]['token']:
                    logger.error('client_id not defined in token section of credentials for cloud %s', cloud)
                    return None
                if 'client_secret' not in data['credentials'][cloud]['token']:
                    logger.error('client_secret not defined in token section of credentials for cloud %s', cloud)
                    return None
                if 'refresh_token' not in data['credentials'][cloud]['token']:
                    logger.error('refresh_token not defined in token section of credentials for cloud %s', cloud)
                    return None
                if 'scope' not in data['credentials'][cloud]['token']:
                    logger.error('scope not defined in token section of credentials for cloud %s', cloud)
                    return None
                if 'url' not in data['credentials'][cloud]['token']:
                    logger.error('url not defined in token section of credentials for cloud %s', cloud)
                    return None

                return (data['credentials'][cloud]['token']['username'],
                        data['credentials'][cloud]['token']['password'],
                        data['credentials'][cloud]['token']['client_id'],
                        data['credentials'][cloud]['token']['client_secret'],
                        data['credentials'][cloud]['token']['refresh_token'],
                        data['credentials'][cloud]['token']['scope'],
                        data['credentials'][cloud]['token']['url'])

    return (None, None, None, None, None, None, None)

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
    r = requests.post(url,
                      headers={'Authorization': 'Bearer %s' % access_token})

    if 'X-Subject-Token' in r.headers:
        return r.headers['X-Subject-Token']
    return None

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
    r = requests.post(url, headers={'content-type': 'application/json'},
                      data=json.dumps(token_body))

    if 'X-Subject-Token' in r.headers:
        return r.headers['X-Subject-Token']
    return None

