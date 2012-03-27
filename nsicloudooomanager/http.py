#!/usr/bin/env python
#-*- coding:utf-8 -*-

from json import dumps, loads
from base64 import decodestring, b64encode
import functools
import cyclone.web
from twisted.internet import defer
from zope.interface import implements
from nsi.cloudooomanager.interfaces.http import IHttp
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
    count = 0

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
        uid = self._load_request_as_json().get('key')
        document = yield self.sam.get(key=uid).resource()
        self.set_header('Content-Type', 'application/json')
        if hasattr(document.data, 'granulated') and not document.data.granulated:
            self.finish(cyclone.web.escape.json_encode({'done':False}))
        else:
            self.finish(cyclone.web.escape.json_encode({'done':True}))

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def post(self):
        request_as_json = self._load_request_as_json()
        callback_url = request_as_json.get('callback') or None
        callback_verb = request_as_json.get('verb') or 'POST'
        filename = request_as_json.get('filename') or urlsplit(request_as_json.get('doc_link')).path.split('/')[-1]
        doc_link = None

        if not request_as_json.get('doc_link'):
            doc = request_as_json.get('doc')
            to_granulate_doc = {"doc":doc, "granulated":False}
            to_granulate_uid = yield self._pre_store_in_sam(to_granulate_doc)
        else:
            to_granulate_uid = yield self._pre_store_in_sam({})
            doc_link = request_as_json.get('doc_link')

        response = self._enqueue_uid_to_granulate(to_granulate_uid, filename, callback_url, callback_verb, doc_link)
        self.set_header('Content-Type', 'application/json')
        self.finish(cyclone.web.escape.json_encode({'key':to_granulate_uid}))

    def _enqueue_uid_to_granulate(self, uid, filename, callback_url, callback_verb, doc_link):
        send_task('nsicloudooomanager.tasks.GranulateDoc', args=(uid, filename, callback_url, callback_verb, doc_link, self.cloudooo_settings,
                                                                self.sam_settings), queue='cloudooo', routing_key='cloudooo')

    def _pre_store_in_sam(self, doc):
        return self.sam.put(value=doc).resource().key

