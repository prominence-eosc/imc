import json
import logging
import sys
import time
import requests

from imc.retry import retry

# Logging
logger = logging.getLogger(__name__)

class OPAClient(object):
    """
    Open Policy Agent helper
    """

    def __init__(self, url=None, timeout=10):
        self._url = url
        self._timeout = timeout

    def set_status(self, cloud, state):
        """
        Write cloud status information in the event of a failure
        """
        data = {}
        data['op'] = 'add'
        data['path'] = '-'
        value = {}
        value['epoch'] = time.time()
        value['reason'] = state
        data['value'] = value

        # Try to patch first. If this fails this probably means we need to do a put.
        try:
            response = requests.patch('%s/v1/data/status/%s/failures' % (self._url, cloud),
                                      data=json.dumps([data]),
                                      timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.warning('Unable to patch cloud status to Open Policy Agent due to "%s"', ex)

        if response.status_code == 404:
            if 'code' in response.json():
                if response.json()['code'] == 'resource_not_found':
                    try:
                        response = requests.put('%s/v1/data/status/%s/failures' % (self._url, cloud),
                                                data=json.dumps([value]),
                                                timeout=self._timeout)
                    except requests.exceptions.RequestException as ex:
                        logger.warning('Unable to put cloud status to Open Policy Agent due to "%s"', ex)

    def set_quotas(self, cloud, instances, cpus, memory):
        """
        Write cloud quotas
        """
        data = {}
        data['cpus'] = cpus
        data['memory'] = memory
        data['instances'] = instances
        data['epoch'] = time.time()

        try:
            response = requests.put('%s/v1/data/status/%s/quota' % (self._url, cloud),
                                    json=data,
                                    timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.warning('Unable to write cloud quotas to Open Policy Agent due to "%s"', ex)

    @retry(tries=4, delay=3, backoff=2)
    def get_clouds(self, data):
        """
        Get list of clouds meeting requirements
        """
        data = {'input':data}

        response = requests.post('%s/v1/data/imc/sites' % self._url,
                                 json=data,
                                 timeout=self._timeout)
        response.raise_for_status()

        if 'result' in response.json():
            return response.json()['result']

        return []

    @retry(tries=4, delay=3, backoff=2)
    def get_ranked_clouds(self, data, clouds):
        """
        Get list of ranked clouds based on preferences
        """
        data = {'input':data}
        data['input']['clouds'] = clouds

        response = requests.post('%s/v1/data/imc/rankedsites' % self._url,
                                 json=data,
                                 timeout=self._timeout)
        response.raise_for_status()

        if 'result' in response.json():
            return response.json()['result']

        return []

    @retry(tries=4, delay=3, backoff=2)
    def get_image(self, data, cloud):
        """
        Get name of an image at the specified site meeting any given requirements
        """
        data = {'input':data}
        data['input']['cloud'] = cloud

        response = requests.post('%s/v1/data/imc/images' % self._url,
                                 json=data,
                                 timeout=self._timeout)
        response.raise_for_status()

        if 'result' in response.json():
            if response.json()['result']:
                return response.json()['result'][0]

        return None

    @retry(tries=4, delay=3, backoff=2)
    def get_flavour(self, data, cloud):
        """
        Get name of a flavour at the specified site meeting requirements. We are given a list
        of flavours and weights, and we pick the flavour with the lowest weight.
        """
        data = {'input':data}
        data['input']['cloud'] = cloud

        response = requests.post('%s/v1/data/imc/flavours' % self._url,
                                 json=data,
                                 timeout=self._timeout)
        response.raise_for_status()

        flavour = None
        if 'result' in response.json():
            if response.json()['result']:
                flavours = sorted(response.json()['result'], key=lambda k: k['weight'])
                flavour = flavours[0]['name']

        return flavour

    def delete_cloud(self, cloud):
        """
        Delete a cloud
        """
        try:
            response = requests.delete('%s/v1/data/clouds/%s' % (self._url, cloud),
                                       timeout=self._timeout)
        except requests.exceptions.RequestException:
            pass

    @retry(tries=4, delay=3, backoff=2)
    def get_cloud(self, cloud):
        """
        Get all info associated with the specified cloud
        """
        response = requests.get('%s/v1/data/clouds/%s' % (self._url, cloud),
                                timeout=self._timeout)
        response.raise_for_status()

        if 'result' in response.json():
            if response.json()['result']:
                return response.json()['result']
        return None

    @retry(tries=4, delay=3, backoff=2)
    def get_all_clouds(self):
        """
        Get all clouds
        """
        response = requests.get('%s/v1/data/clouds' % self._url,
                                timeout=self._timeout)
        response.raise_for_status()

        if 'result' in response.json():
            if response.json()['result']:
                return response.json()['result']
        return None

    @retry(tries=4, delay=3, backoff=2)
    def get_flavours(self, cloud):
        """
        Get all flavours associated with the specified cloud
        """
        response = requests.get('%s/v1/data/clouds/%s/flavours' % (self._url, cloud),
                                timeout=self._timeout)
        response.raise_for_status()

        if 'result' in response.json():
            if response.json()['result']:
                return response.json()['result']
        return None        

    @retry(tries=4, delay=3, backoff=2)
    def get_images(self, cloud):
        """
        Get all images associated with the specified cloud
        """
        response = requests.get('%s/v1/data/clouds/%s/images' % (self._url, cloud),
                                timeout=self._timeout)
        response.raise_for_status()

        if 'result' in response.json():
            if response.json()['result']:
                return response.json()['result']
        return None

    def set_flavours(self, cloud, data):
        """
        Set all flavours associated with the specified cloud
        """
        try:
            response = requests.put('%s/v1/data/clouds/%s/flavours' % (self._url, cloud),
                                    json=data,
                                    timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to write cloud flavours to Open Policy Agent due to "%s"', ex)
            return False
        return True
            
    def set_images(self, cloud, data):
        """
        Set all images associated with the specified cloud
        """
        try:
            response = requests.put('%s/v1/data/clouds/%s/images' % (self._url, cloud),
                                    json=data,
                                    timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to write cloud images to Open Policy Agent due to "%s"', ex)
            return False
        return True

    def set_cloud(self, cloud, data):
        """
        Set all info associated with the specified cloud
        """
        try:
            response = requests.put('%s/v1/data/clouds/%s' % (self._url, cloud),
                                    json=data,
                                    timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to write cloud info to Open Policy Agent due to "%s"', ex)
            return False
        return True

    def set_update_time(self, cloud):
        """
        Set when the specified cloud was last updated
        """
        data = {}
        data['epoch'] = time.time()

        try:
            response = requests.put('%s/v1/data/clouds/%s/updated' % (self._url, cloud),
                                    json=data,
                                    timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to write cloud update time to Open Policy Agent due to "%s"', ex)
            return False
        return True

    @retry(tries=4, delay=3, backoff=2)
    def get_cloud_update_time(self, cloud):
        """
        Return when the cloud was last updated
        """
        response = requests.get('%s/v1/data/clouds/%s/updated' % (self._url, cloud),
                                timeout=self._timeout)
        response.raise_for_status()

        if 'result' in response.json():
            if 'epoch' in response.json()['result']:
                return response.json()['result']['epoch']
        return 0

    @retry(tries=4, delay=3, backoff=2)
    def get_quota_update_time(self, cloud):
        """
        Return when the cloud quotas were last updated
        """
        response = requests.get('%s/v1/data/status/%s/quota' % (self._url, cloud),
                                timeout=self._timeout)
        response.raise_for_status()

        if 'result' in response.json():
            if 'epoch' in response.json()['result']:
                return response.json()['result']['epoch']
        return 0

    def remove_failure_item(self, cloud, event_id):
        """
        Remove a failure event for the specified cloud
        """
        data = {}
        data['op'] = 'remove'
        data['path'] = ''

        try:
            response = requests.patch('%s/v1/data/status/%s/failures/%d' % (self._url, cloud, event_id),
                                      data=json.dumps([data]),
                                      timeout=self._timeout)
        except requests.exceptions.RequestException:
            return False

        return True

    def remove_old_failures(self):
        """
        Remove old failure events
        """
        # Get list of clouds
        try:
            response = requests.get('%s/v1/data/status' % self._url, timeout=self._timeout)
        except requests.exceptions.RequestException:
            return False

        if response.status_code != 200:
            return False

        clouds = []
        if 'result' in response.json():
            for cloud in response.json()['result']:
                clouds.append(cloud)

        for cloud in clouds:
            logger.info('Working on cloud %s', cloud)
            checking = True
            events_removed = 0
            while checking:
                try:
                    response = requests.get('%s/v1/data/status/%s' % (self._url, cloud),
                                            timeout=self._timeout)
                except requests.exceptions.RequestException:
                    continue

                if response.status_code != 200:
                    continue

                if 'result' in response.json():
                    if 'failures' in response.json()['result']:
                        failures = response.json()['result']['failures']
                        old = 0
                        event_id = 0
                        for failure in failures:
                            if time.time() - failure['epoch'] > 12*60*60:
                                old += 1
                                self.remove_failure_item(cloud, event_id)
                                events_removed += 1
                                break
                            event_id += 1

                        if old == 0:
                            checking = False
                    else:
                        checking = False
                else:
                    checking = False
            logger.info('Removed %d failure events from cloud %s', events_removed, cloud)

        return False
