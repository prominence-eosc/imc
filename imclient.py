#!/usr/bin/python
import requests

class IMClient(object):
    """
    Infrastructure Manager client helper
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
            except IOError as ex:
                return (1, ex)
        self._headers = {'AUTHORIZATION':str(content.replace('____n', '\\\\n'))}
        return (0, None)

    def return_headers(self):
        """
        Return the headers used for making requests
        """
        return self._headers

    def getstate(self, infra_id, timeout):
        """
        Get overall infrastructure status
        """
        url = '%s/infrastructures/%s/state' % (self._url, infra_id)
        try:
            response = requests.get(url, headers=self._headers, timeout=timeout)
        except requests.exceptions.Timeout:
            return ('timedout', None)
        except requests.exceptions.RequestException:
            return ('timedout', None)

        if response.status_code != 200:
            return (None, response.text)
        return (response.json()['state']['state'], response.text)

    def getstates(self, infra_id, timeout):
        """
        Get infrastructure status - overall & individual VMs
        """
        url = '%s/infrastructures/%s/state' % (self._url, infra_id)
        try:
            response = requests.get(url, headers=self._headers, timeout=timeout)
        except requests.exceptions.Timeout as ex:
            return ('timedout', ex)
        except requests.exceptions.RequestException as ex:
            return ('timedout', ex)

        if response.status_code != 200:
            return (None, response.text)
        return (response.json(), response.text)

    def getdata(self, infra_id, timeout):
        """
        Get infrastructure status
        """
        url = '%s/infrastructures/%s/data' % (self._url, infra_id)
        try:
            response = requests.get(url, headers=self._headers, timeout=timeout)
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
        url = '%s/infrastructures/%s' % (self._url, infra_id)
        try:
            response = requests.delete(url, headers=self._headers, timeout=timeout)
        except requests.exceptions.Timeout:
            return (2, None)
        except requests.exceptions.RequestException:
            return (2, None)

        if response.status_code == 200:
            return (0, response.text)
        return (1, response.text)

    def create(self, radl, timeout):
        """
        Create infrastructure
        """
        headers = self._headers.copy()
        headers['Content-Type'] = 'text/plain'

        # We use the async parameter so that we don't wait for VMs to be created
        params = {}
        params['async'] = 1

        url = '%s/infrastructures' % self._url
        try:
            response = requests.post(url, params=params, headers=headers, timeout=timeout, data=radl)
        except requests.exceptions.Timeout:
            return (None, 'timedout')
        except requests.exceptions.RequestException as ex:
            return (None, ex)

        if response.status_code == 200:
            return (response.text.split('/infrastructures/')[1], None)
        return (None, response.text)

    def reconfigure_new(self, infra_id, radl, timeout):
        """
        Reconfigure infrastructure with new RADL
        """
        headers = self._headers.copy()
        headers['Content-Type'] = 'text/plain'

        url = '%s/infrastructures/%s/reconfigure' % (self._url, infra_id)
        try:
            response = requests.put(url, headers=self._headers, timeout=timeout, data=radl)
        except requests.exceptions.Timeout:
            return (2, None)
        except requests.exceptions.RequestException:
            return (2, None)

        if response.status_code == 200:
            return (0, response.text)
        return (1, response.text)

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

    def remove_resource(self, infra_id, vm_id, timeout):
        """
        Remove a resource (VM) from an infrastructure
        """
        url = '%s/infrastructures/%s/vms/%d' % (self._url, infra_id, vm_id)
        try:
            response = requests.delete(url, headers=self._headers, timeout=timeout)
        except requests.exceptions.Timeout:
            return (2, None)
        except requests.exceptions.RequestException:
            return (2, None)

        if response.status_code == 200:
            return (0, response.text)
        return (1, response.text)

    def get_vm_info(self, infra_id, vm_id, timeout):
        """
        Get info about a VM
        """
        url = '%s/infrastructures/%s/vms/%d' % (self._url, infra_id, vm_id)

        headers = self._headers.copy()
        headers['Accept'] = 'application/json'

        try:
            response = requests.get(url, headers=headers, timeout=timeout)
        except requests.exceptions.Timeout:
            return (2, None)
        except requests.exceptions.RequestException:
            return (2, None)

        if response.status_code == 200:
            return (0, response.json())
        return (1, response.text)

    def add_resource(self, infra_id, radl, timeout):
        """
        Add a resource (VM) to a infrastructure
        """
        headers = self._headers.copy()
        headers['Content-Type'] = 'text/plain'

        # We use the async parameter so that we don't wait for VMs to be created
        params = {}
        params['async'] = 1

        url = '%s/infrastructures/%s' % (self._url, infra_id)
        try:
            response = requests.post(url, params=params, headers=headers, timeout=timeout, data=radl)
        except requests.exceptions.Timeout:
            return (2, 'timedout')
        except requests.exceptions.RequestException as ex:
            return (2, ex)

        if response.status_code == 200:
            return (0, None)
        return (1, response.text)
