#!/usr/bin/python
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
