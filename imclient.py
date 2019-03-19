#!/usr/bin/python
import time
import requests

class IMClient(object):
    """
    Infrastructure Manager client helperclass
    """

    def __init__(self, url=None, auth=None, data=None):
        self._url = url
        self._auth = auth
        self._data = data
        self._headers = None

    def getauth(self):
        """
        Generate auth header
        """
        if self._auth is None and self._data is not None:
            content = self._data
        else:
            try:
                with open(self._auth) as auth_file:
                    content = '\\n'.join(auth_file.read().splitlines())
            except IOError as e:
                return (1, e)
        self._headers = {'AUTHORIZATION':str(content.replace('____n', '\\\\n'))}
        return (0, None)

    def return_headers(self):
        return self._headers

    def getstate(self, infra_id, timeout):
        """
        Get infrastructure status
        """
        try:
            response = requests.get(self._url + '/infrastructures/' + infra_id + '/state', headers=self._headers, timeout=timeout)
        except requests.exceptions.Timeout:
            return ('timedout', None)
        except requests.exceptions.RequestException:
            return ('timedout', None)

        if response.status_code != 200:
            return (None, response.text)
        return (response.json()['state']['state'], response.text)

    def getdata(self, infra_id, timeout):
        """
        Get infrastructure status
        """
        try:
            response = requests.get(self._url + '/infrastructures/' + infra_id + '/data', headers=self._headers, timeout=timeout)
        except requests.exceptions.Timeout:
            return ('timedout', None)
        except requests.exceptions.RequestException:
            return ('timedout', None)

        if response.status_code != 200:
            return (None, response.text)
        return (response.json(), response.text)

    def destroy(self, infra_id, timeout):
        """
        Destroy infrastructure
        """
        try:
            response = requests.delete(self._url + '/infrastructures/' + infra_id, headers=self._headers, timeout=timeout)
        except requests.exceptions.Timeout:
            return (2, None)
        except requests.exceptions.RequestException:
            return (2, None)

        if response.status_code == 200:
            return (0, response.text)
        return (1, response.text)

    def create(self, filename, timeout):
        """
        Create infrastructure
        """
        with open(filename) as data:
            radl = data.read()

        headers = self._headers.copy()
        headers['Content-Type'] = 'text/plain'

        # We use the async parameter so that we don't wait for VMs to be created
        params = {}
        params['async'] = 1

        try:
            response = requests.post(self._url + '/infrastructures', params=params, headers=headers, timeout=timeout, data=radl)
        except requests.exceptions.Timeout:
            return (None, 'timedout')
        except requests.exceptions.RequestException as e:
            return (None, e)

        if response.status_code == 200:
            return (response.text.split('/infrastructures/')[1], None)
        return (None, response.text)

    def reconfigure(self, infra_id, timeout):
        """
        Reconfigure infrastructure
        """
        try:
            response = requests.put(self._url + '/infrastructures/' + infra_id + '/reconfigure', headers=self._headers, timeout=timeout)
        except requests.exceptions.Timeout:
            return (2, None)
        except requests.exceptions.RequestException:
            return (2, None)

        if response.status_code == 200:
            return (0, response.text)
        return (1, response.text)

    def getcontmsg(self, infra_id, timeout):
        """
        Get contextualization message
        """
        try:
            response = requests.get(self._url + '/infrastructures/' + infra_id + '/contmsg', headers=self._headers, timeout=timeout)
        except requests.exceptions.Timeout:
            return None
        except requests.exceptions.RequestException:
            return None

        return response.text
