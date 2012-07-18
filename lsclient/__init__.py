import requests
import json

def apply_attrs(obj, data):
    for k, v in data.items():
        setattr(obj, k, v)


class LSException(Exception):

    def __init__(self, data):
        apply_attrs(self, data)


class LSNoMatch(LSException):

    def __repr__(self):
        return "<LSNoMatch(%s:%s)>" % (self.dataset,
                                       self.key)


class LSInvalid(LSException):

    def __repr__(self):
        return "<LSInvalid(%s:%s)>" % (self.dataset,
                                       self.key)


class LSValue(object):

    def __init__(self, dataset, data):
        self._dataset = dataset
        apply_attrs(self, data)

    def __repr__(self):
        return "<LSValue(%s:%s:%s)>" % (self._dataset.name,
                                        self.id, self.value)


class LSLink(object):

    INVALID = "INVALID"
    NEW = "NEW"

    def __init__(self, dataset, data):
        self._dataset = dataset
        apply_attrs(self, data)

    def __repr__(self):
        return "<LSLink(%s:%s:%s:%s)>" % (self._dataset.name,
                                       self.id, self.key, self.is_matched)


class LSDataset(object):

    def __init__(self, dataset, 
            host='http://linker.pudo.org',
            api_key=None):
        self.host = host
        self.name = dataset
        self.api_key = api_key
        self._fetch()

    @property
    def _session(self):
        if not hasattr(self, '_session_obj'):
            headers = {'Accept': 'application/json',
                       'Content-Type': 'application/json'}
            if self.api_key is not None:
                headers['Authorization'] = self.api_key
            self._session_obj = requests.session(headers=headers)
        return self._session_obj

    def _get(self, path, params={}):
        response = self._session.get(self.host + '/' + self.name + path,
                params=params)
        return response.status_code, response.json

    def _post(self, path, data={}):
        data = json.dumps(data)
        response = self._session.post(self.host + '/' + self.name + path,
                allow_redirects=True,
                data=data)
        return response.status_code, json.loads(response.content)

    def _fetch(self):
        code, data = self._get('')
        print code, data
        apply_attrs(self, data)

    def get_value(self, id=None, value=None):
        assert id or value, "Need to give an ID or a value!"
        if id is not None:
            code, val = self._get('/values/%s' % id)
        else:
            code, val = self._get('/value', params={'value': value})
        if code != 200:
            raise LSException(val or {})
        return LSValue(self, val)

    def add_value(self, value, context={}):
        code, val = self._post('/values', data={'value': value})
        if code == 400:
            raise LSException(val)
        return LSValue(self, val)

    def ensure_value(self, value):
        try:
            return self.get_value(value=value)
        except LSException, ex:
            return self.add_value(value=value)

    def values(self):
        code, vals = self._get('/values')
        return [LSValue(self, v) for v in vals]

    def get_link(self, id=None, key=None):
        assert id or value, "Need to give an ID or a ket!"
        if id is not None:
            code, val = self._get('/links/%s' % id)
        else:
            code, val = self._get('/link', params={'key': value})
        if code != 200:
            raise LSException(val)
        return LSLink(self, val)

    def links(self):
        code, vals = self._get('/links')
        return [LSLink(self, v) for v in vals]

    def lookup(self, key, context={}, readonly=False):
        code, val = self._post('/lookup', 
                data={'key': key,
                      'readonly': readonly})
        if code == 404:
            raise LSNoMatch(val)
        elif code == 418:
            raise LSInvalid(val)
        else:
            return LSValue(self, val.get('value'))

    def match(self, link_id, value_id):
        code, val = self._post('/links/%s/match' % link_id, 
                        data={'choice': value_id,
                              'value': ''})
        if code != 200:
            raise LSException(val)
        return None

    def __repr__(self):
        return "<LSDataset(%s)" % self.name


if __name__ == "__main__":
    ds = LSDataset('offenesparlament', 'http://localhost:5000')

