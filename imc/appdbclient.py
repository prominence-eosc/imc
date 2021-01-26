"""Interactions with EGI's AppDB"""
import logging
import requests
import xmltodict

# Logging
logger = logging.getLogger(__name__)

def appdb_call(query):
    try:
        response = requests.get('https://appdb.egi.eu' + query)
        data = response.text
    except:
        logger.error('Unable to query AppDB with query: %s', query)
        return {}

    data.replace('\n', '')
    return xmltodict.parse(data)

def get_cloud_status_appdb():
    data = appdb_call('/rest/1.0/va_providers/nova')

    output = {}
    if 'appdb:appdb' not in data:
        return output

    if 'virtualization:provider' in data['appdb:appdb']:
        for provider in data['appdb:appdb']['virtualization:provider']:
            if '@service_status' in provider:
                output[provider['provider:name']] = provider['@service_status']

    return output
