import logging
import sys
import time
import requests

# Logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
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
        Write cloud status information
        """
        data = {}
        data['epoch'] = time.time()

        try:
            response = requests.put('%s/v1/data/status/failures/%s' % (self._url, cloud),
                                    json=data,
                                    timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.warning('Unable to write cloud status to Open Policy Agent due to "%s"', ex)

    def get_clouds(self, data):
        """
        Get list of clouds meeting requirements
        """
        data = {'input':data}

        try:
            response = requests.post('%s/v1/data/imc/sites' % self._url,
                                     json=data,
                                     timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to get list of sites from Open Policy Agent due to "%s"', ex)
            return []

        if 'result' in response.json():
            return response.json()['result']

        return []

    def get_ranked_clouds(self, data, clouds):
        """
        Get list of ranked clouds based on preferences
        """
        data = {'input':data}
        data['input']['clouds'] = clouds

        try:
            response = requests.post('%s/v1/data/imc/rankedsites' % self._url,
                                     json=data,
                                     timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to get list of ranked sites from Open Policy Agent due to "%s"', ex)
            return []

        if 'result' in response.json():
            return response.json()['result']

        return []

    def get_image(self, data, cloud):
        """
        Get name of an image at the specified site meeting any given requirements
        """
        data = {'input':data}
        data['input']['cloud'] = cloud

        try:
            response = requests.post('%s/v1/data/imc/images' % self._url,
                                     json=data,
                                     timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to get image from Open Policy Agent due to "%s"', ex)
            return None

        if 'result' in response.json():
            if response.json()['result']:
                return response.json()['result'][0]

        return None

    def get_flavour(self, data, cloud):
        """
        Get name of a flavour at the specified site meeting requirements. We are given a list
        of flavours and weights, and we pick the flavour with the lowest weight.
        """
        data = {'input':data}
        data['input']['cloud'] = cloud

        try:
            response = requests.post('%s/v1/data/imc/flavours' % self._url,
                                     json=data,
                                     timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to get flavour from Open Policy Agent due to "%s"', ex)
            return None

        flavour = None
        if 'result' in response.json():
            if response.json()['result']:
                flavours = sorted(response.json()['result'], key=lambda k: k['weight'])
                flavour = flavours[0]['name']

        return flavour

    def get_flavours(self, cloud):
        """
        Get all flavours associated with the specified cloud
        """
        try:
            response = requests.get('%s/v1/data/clouds/%s/flavours' % (self._url, cloud),
                                    timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to get flavours from Open Policy Agent due to "%s"', ex)
            return None        

        if 'result' in response.json():
            if response.json()['result']:
                return response.json()['result']
        return None        

    def get_images(self, cloud):
        """
        Get all images associated with the specified cloud
        """
        try:
            response = requests.get('%s/v1/data/clouds/%s/images' % (self._url, cloud),
                                    timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to get images from Open Policy Agent due to "%s"', ex)
            return None

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

    def get_update_time(self, cloud):
        """
        Return when the cloud was last updated
        """
        try:
            response = requests.get('%s/v1/data/clouds/%s/updated' % (self._url, cloud),
                                    timeout=self._timeout)
        except requests.exceptions.RequestException as ex:
            logger.critical('Unable to get cloud updated time from Open Policy Agent due to "%s"', ex)
            return 1

        if 'result' in response.json():
            if 'epoch' in response.json()['result']:
                return response.json()['result']['epoch']
        return 0
