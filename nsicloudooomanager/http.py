#!/usr/bin/env python
#-*- coding:utf-8 -*-

from json import dumps, loads
from base64 import decodestring, b64encode
import functools
import cyclone.web
from twisted.internet import defer
from zope.interface import implements
from nsicloudooomanager.interfaces.http import IHttp
from restfulie import Restfulie
from celery.execute import send_task
from urlparse import urlsplit

def auth(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        auth_type, auth_data = self.request.headers.get("Authorization").split()
        if not auth_type == "Basic":
            raise cyclone.web.HTTPAuthenticationRequired("Basic", realm="Restricted Access")
        user, password = decodestring(auth_data).split(":")
        # authentication itself
        if not self.settings.auth.authenticate(user, password):
            raise cyclone.web.HTTPError(401, "Unauthorized")
        return method(self, *args, **kwargs)
    return wrapper


class HttpHandler(cyclone.web.RequestHandler):

    implements(IHttp)
    no_keep_alive = True

    def _get_current_user(self):
        auth = self.request.headers.get("Authorization")
        if auth:
          return decodestring(auth.split(" ")[-1]).split(":")

    def _load_request_as_json(self):
        return loads(self.request.body)

    def _load_sam_config(self):
        self.sam_settings = {'url': self.settings.sam_url, 'auth': [self.settings.sam_user, self.settings.sam_pass]}

    def _load_cloudooo_config(self):
        self.cloudooo_settings = {'url':self.settings.cloudooo_url}

    def __init__(self, *args, **kwargs):
        cyclone.web.RequestHandler.__init__(self, *args, **kwargs)
        self._load_sam_config()
        self._load_cloudooo_config()
        self.sam = Restfulie.at(self.sam_settings['url']).auth(*self.sam_settings['auth']).as_('application/json')

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def get(self):
        doc_key = self._load_request_as_json().get('doc_key')
        if doc_key:
            keys = yield self._get_grains_keys(doc_key)
            self.set_header('Content-Type', 'application/json')
            self.finish(cyclone.web.escape.json_encode(keys))
            return
        uid = self._load_request_as_json().get('key')
        if not uid:
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        response = yield self.sam.get(key=uid)
        if response.code == '404':
            raise cyclone.web.HTTPError(404, 'Key not found in SAM.')
        elif response.code == '401':
            raise cyclone.web.HTTPError(401, 'SAM user and password not match.')
        elif response.code == '500':
            raise cyclone.web.HTTPError(500, 'Error while connecting to SAM.')
        document = response.resource()
        self.set_header('Content-Type', 'application/json')
        if hasattr(document.data, 'granulated') and document.data.granulated:
            self.finish(cyclone.web.escape.json_encode({'done':True}))
        else:
            self.finish(cyclone.web.escape.json_encode({'done':False}))

    def _get_grains_keys(self, doc_key):
        document_uid = doc_key
        response = self.sam.get(key=document_uid)
        if response.code == '404':
            raise cyclone.web.HTTPError(404, 'Key not found in SAM.')
        sam_entry = loads(response.body)
        grains = sam_entry['data']['grains_keys']
        return grains

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def post(self):
        request_as_json = self._load_request_as_json()
        callback_url = request_as_json.get('callback') or None
        callback_verb = request_as_json.get('verb') or 'POST'
        if not request_as_json.get('filename') and not request_as_json.get('doc_link'):
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        filename = request_as_json.get('filename') or urlsplit(request_as_json.get('doc_link')).path.split('/')[-1]
        doc_link = None

        if request_as_json.get('doc'):
            doc = request_as_json.get('doc')
            to_granulate_doc = {"doc":doc, "granulated":False}
            to_granulate_uid = yield self._pre_store_in_sam(to_granulate_doc)
        elif request_as_json.get('sam_uid'):
            to_granulate_uid = yield request_as_json.get('sam_uid')
            response = self.sam.get(key=to_granulate_uid)
            if response.code == '404':
                raise cyclone.web.HTTPError(404, 'Key not found at SAM.')
        elif request_as_json.get('doc_link'):
            to_granulate_uid = yield self._pre_store_in_sam({"granulated":False})
            doc_link = request_as_json.get('doc_link')
        else:
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        response = self._enqueue_uid_to_granulate(to_granulate_uid, filename, callback_url, callback_verb, doc_link)
        self.set_header('Content-Type', 'application/json')
        self.finish(cyclone.web.escape.json_encode({'doc_key':to_granulate_uid}))

    def _enqueue_uid_to_granulate(self, uid, filename, callback_url, callback_verb, doc_link):
        send_task('nsicloudooomanager.tasks.GranulateDoc', args=(uid, filename, callback_url, callback_verb, doc_link, self.cloudooo_settings,
                                                                self.sam_settings), queue='cloudooo', routing_key='cloudooo')

    def _pre_store_in_sam(self, doc):
        response = self.sam.put(value=doc)
        if response.code == '500':
            raise cyclone.web.HTTPError(500, 'Error while connecting to SAM.')
        elif response.code == '404':
            raise cyclone.web.HTTPError(404, 'Key not found in SAM.' )
        elif response.code == '401':
            raise cyclone.web.HTTPError(401, 'SAM user and password not match.')
        return self.sam.put(value=doc).resource().key

