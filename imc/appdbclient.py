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

def get_clouds_for_vo(vo):
    data = appdb_call('/rest/1.0/sites?listmode=details&flt=%%3Dvo.name%%3A%s' % vo)

    sites = []
    if 'appdb:appdb' not in data:
        return sites

    if 'appdb:site' in data['appdb:appdb']:
        for site in data['appdb:appdb']['appdb:site']:
            if 'site:service' in site:
                if type(site['site:service']) == type([]):
                    for service in site['site:service']:
                        if '@type' in service:
                            if service['@type'] == 'openstack':
                                if site['@name'] not in sites:
                                    sites.append(site['@name'])
                else:
                    if '@type' in site['site:service']:
                        if site['site:service']['@type'] == 'openstack':
                            if site['@name'] not in sites:
                                sites.append(site['@name'])

    return sites

