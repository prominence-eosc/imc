import logging
import os
import defusedxml.ElementTree as ET
from six.moves.urllib import parse
import requests

from imc import config
from imc import tokens

# Configuration
CONFIG = config.get_config()

# Logging
logger = logging.getLogger(__name__)

TIMEOUT = 10

def get_unscoped_token(os_auth_url, access_token):
    """
    Get an unscopped token, trying various protocol names if needed
    """
    protocols = ["openid", "oidc"]
    for p in protocols:
        try:
            unscoped_token, user_domain_name = retrieve_unscoped_token(os_auth_url, access_token, p)
            return unscoped_token, user_domain_name, p
        except RuntimeError as err:
            pass
    raise RuntimeError("Unable to get an unscoped token")

def retrieve_unscoped_token(os_auth_url, access_token, protocol="openid"):
    """
    Request an unscopped token
    """
    url = get_keystone_url(
        os_auth_url,
        "/v3/OS-FEDERATION/identity_providers/egi.eu/protocols/%s/auth" % protocol,
    )
    r = requests.post(url, headers={"Authorization": "Bearer %s" % access_token}, timeout=TIMEOUT)
    if r.status_code != requests.codes.created:
        raise RuntimeError("Unable to get an unscoped token")
    else:
        user_domain_name = None
        if 'token' in r.json():
            if 'user' in r.json()['token']:
                if 'domain' in r.json()['token']['user']:
                    if 'name' in r.json()['token']['user']['domain']:
                        user_domain_name = r.json()['token']['user']['domain']['name']
        return (r.headers["X-Subject-Token"], user_domain_name)

def find_endpoint(service_type, production=True, monitored=True, site=None):
    """
    Find endpoints
    """
    q = {"method": "get_service_endpoint", "service_type": service_type}
    if monitored:
        q["monitored"] = "Y"
    if site:
        q["sitename"] = site
        sites = [site]
    else:
        sites = get_sites()
    url = "?".join([CONFIG.get('egi', 'goc_url'), parse.urlencode(q)])
    r = requests.get(url, timeout=TIMEOUT)
    endpoints = []
    if r.status_code == 200:
        root = ET.fromstring(r.text)
        for sp in root:
            if production:
                prod = sp.find("IN_PRODUCTION").text.upper()
                if prod != "Y":
                    continue
            os_url = sp.find("URL").text
            ep_site = sp.find('SITENAME').text
            if ep_site not in sites:
                continue
            endpoints.append([sp.find("SITENAME").text, service_type, os_url])
    return endpoints

def get_projects(os_auth_url, unscoped_token):
    """
    Get projects
    """
    url = get_keystone_url(os_auth_url, "/v3/auth/projects")
    r = requests.get(url, headers={"X-Auth-Token": unscoped_token}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()["projects"]

def get_regions(os_auth_url, unscoped_token):
    """
    Get regions
    """
    url = get_keystone_url(os_auth_url, "/v3/regions")
    r = requests.get(url, headers={"X-Auth-Token": unscoped_token}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()["regions"]

def projects(site, access_token):
    """
    Generate a list of sites running OpenStack supporting the user
    """
    project_list = []
    for endpoint in find_endpoint("org.openstack.nova", site=site):
        os_auth_url = endpoint[2]
        unscoped_token, user_domain_name, protocol = get_unscoped_token(os_auth_url, access_token)

        # Get region
        regions = get_regions(os_auth_url, unscoped_token)
        region_id = None
        for region in regions:
            region_id = region['id']

        # Generate list of projects
        for project in get_projects(os_auth_url, unscoped_token):
            if project["enabled"]:
                project_list.append(
                    {   
                        "project_id": project["id"],
                        "project_name": project["name"],
                        "project_domain_id": project["domain_id"],
                        "user_domain_name": user_domain_name,
                        "site": endpoint[0],
                        "protocol": protocol,
                        "auth_type": "3.x_oidc_access_token",
                        "identity_provider": "egi.eu",
                        "auth_url": os_url_strip(os_auth_url),
                        "region": region_id
                    }
                )
    return project_list

def oidc_discover(checkin_url):
    # discover oidc endpoints
    r = requests.get(checkin_url + "/.well-known/openid-configuration", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def token_refresh(
    checkin_client_id, checkin_client_secret, checkin_refresh_token, token_url
):
    """Mananages Check-in tokens"""
    refresh_data = {
        "client_id": checkin_client_id,
        "client_secret": checkin_client_secret,
        "grant_type": "refresh_token",
        "refresh_token": checkin_refresh_token,
        "scope": "openid email profile offline_access",
    }
    r = requests.post(
        token_url, auth=(checkin_client_id, checkin_client_secret), data=refresh_data, timeout=TIMEOUT
    )
    r.raise_for_status()
    return r.json()

def os_url_strip(url):
    pieces = url.split('/')
    return '%s//%s' % (pieces[0], pieces[2])

def get_sites():
    """
    Get list of sites from the GOC DB
    """
    q = {"method": "get_site_list", "certification_status": "Certified"}
    url = "?".join([CONFIG.get('egi', 'goc_url'), parse.urlencode(q)])
    r = requests.get(url, timeout=TIMEOUT)
    sites = []
    if r.status_code == 200:
        root = ET.fromstring(r.text)
        for s in root:
            sites.append(s.attrib.get('NAME'))
    return sites

def get_keystone_url(os_auth_url, path):
    url = parse.urlparse(os_auth_url)
    prefix = url.path.rstrip("/")
    if prefix.endswith("v2.0") or prefix.endswith("v3"):
        prefix = os.path.dirname(prefix)
    path = os.path.join(prefix, path)
    return parse.urlunparse((url[0], url[1], path, url[3], url[4], url[5]))

def refresh_access_token(
    checkin_client_id, checkin_client_secret, checkin_refresh_token, checkin_url
):
    oidc_ep = oidc_discover(checkin_url)
    return token_refresh(
        checkin_client_id,
        checkin_client_secret,
        checkin_refresh_token,
        oidc_ep["token_endpoint"],
    )["access_token"]

def get_egi_clouds(access_token):
    """
    Generate a list of EGI FedCloud OpenStack clouds the user is able to use
    """
    clouds = []

    # Get full list of OpenStack sites
    sites = find_endpoint("org.openstack.nova", production=True, monitored=True)
    logger.info('Got list of %d sites to check', len(sites))

    # Get details for each site
    for site in sites:
        site_name = site[0]

        logger.info('Checking site %s', site_name)

        project_list = []
        try:
            project_list = projects(site_name, access_token)
        except Exception as err:
            pass

        if project_list:
            clouds.extend(project_list)

    logger.info('Finished checking each site')

    return clouds

def egi_clouds_update(identity, db):
    """
    Update EGI Federated Clouds for the specified user
    """
    # Get EGI access token
    logger.info('Getting EGI access token in egi_clouds_update')
    token = tokens.get_token(None, identity, db, None)

    if not token:
        logger.info('No valid token so will not try to get list of clouds')
        return

    # Get list of clouds & their details
    logger.info('Getting EGI Federated Cloud sites')
    try:
        clouds = get_egi_clouds(token)
    except Exception as err:
        clouds = []
        logger.error('Got unexpected exception finding EGI clouds: %s', err)

    clouds_list = []
    for cloud in clouds:
        clouds_list.append(cloud['site'])

    logger.info('Got %d clouds: %s', len(clouds_list), ','.join(clouds_list))

    # Add clouds to database
    count = 0
    for cloud in clouds:
        if cloud['site'] in CONFIG.get('egi', 'blacklist').split(','):
            logger.info('Ignoring cloud %s as it is in the blacklist', cloud['site'])
        else:
            status = db.set_egi_cloud(identity,
                                      cloud['site'],
                                      cloud['auth_url'],
                                      cloud['project_id'],
                                      cloud['project_domain_id'],
                                      cloud['user_domain_name'],
                                      cloud['region'],
                                      cloud['protocol'])
            if status:
               count = count + 1

    logger.info('Added %d clouds to the database', count)

    # Disable any clouds if necessary, for example if a user has left a VO
    if clouds_list:
        db.disable_egi_clouds(identity, clouds_list)

    return
