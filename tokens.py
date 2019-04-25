#!/usr/bin/python

import json
import logging
import sys
import time
import requests

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
        db_delete_token(cloud)

        # Save token to DB
        db_set_token(cloud, token, expiry, creation)
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
            return (data['credentials'][cloud]['username'],
                    data['credentials'][cloud]['password'],
                    data['credentials'][cloud]['client_id'],
                    data['credentials'][cloud]['client_secret'],
                    data['credentials'][cloud]['refresh_token'],
                    data['credentials'][cloud]['scope'],
                    data['credentials'][cloud]['url'])

    return (None, None, None, None, None, None, None)

