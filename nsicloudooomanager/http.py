#!/usr/bin/env python
#-*- coding:utf-8 -*-

from json import dumps, loads
from base64 import decodestring, b64encode
import functools
import cyclone.web
from twisted.internet import defer
from twisted.python import log
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
            log.msg("Authentication failed.")
            log.msg("User '%s' and password '%s' not known." % (user, password))
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
        self._task_queue = self.settings.task_queue
        self.sam = Restfulie.at(self.sam_settings['url']).auth(*self.sam_settings['auth']).as_('application/json')

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def get(self):
        doc_key = self._load_request_as_json().get('doc_key')
        metadata = self._load_request_as_json().get('metadata')
        if doc_key and metadata:
            key = yield self._get_metadata_key(doc_key)
            self.set_header('Content-Type', 'application/json')
            log.msg("Found the metadata key for the document with key: %s" % doc_key)
            self.finish(cyclone.web.escape.json_encode({'metadata_key':key}))
            return
        elif doc_key: # find the grains keys for the document at 'doc_key'
            keys = yield self._get_grains_keys(doc_key)
            self.set_header('Content-Type', 'application/json')
            log.msg("Found the grains keys for the document with key: %s" % doc_key)
            self.finish(cyclone.web.escape.json_encode(keys))
            return

        uid = self._load_request_as_json().get('key')
        if not uid:
            log.msg("GET failed.")
            log.msg("Request didn't have a key to check.")
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        response = yield self.sam.get(key=uid)
        if response.code == '404':
            log.msg("GET failed!")
            log.msg("Couldn't find any value for the key: %s" % key)
            raise cyclone.web.HTTPError(404, 'Key not found in SAM.')
        elif response.code == '401':
            log.msg("GET failed!")
            log.msg("Couldn't authenticate with SAM.")
            raise cyclone.web.HTTPError(401, 'SAM user and password not match.')
        elif response.code == '500':
            log.msg("GET failed!")
            log.msg("Couldn't connecting to SAM.")
            raise cyclone.web.HTTPError(500, 'Error while connecting to SAM.')
        document = response.resource()
        self.set_header('Content-Type', 'application/json')
        if hasattr(document.data, 'granulated') and document.data.granulated:
            log.msg('Document with key %s is done.' % uid)
            self.finish(cyclone.web.escape.json_encode({'done':True}))
        else:
            log.msg('Document with key %s isnt done.' % uid)
            self.finish(cyclone.web.escape.json_encode({'done':False}))

    def _get_metadata_key(self, doc_key):
        response = self.sam.get(key=doc_key)
        if response.code == '404':
            log.msg("GET failed!")
            log.msg("Couldn't find any value for the key: %s" % key)
            raise cyclone.web.HTTPError(404, 'Key not found in SAM.')
        sam_entry = loads(response.body)
        metadata_key = sam_entry['data'].get('metadata_key')
        return metadata_key

    def _get_grains_keys(self, doc_key):
        document_uid = doc_key
        response = self.sam.get(key=document_uid)
        if response.code == '404':
            log.msg("GET failed!")
            log.msg("Couldn't find any value for the key: %s" % key)
            raise cyclone.web.HTTPError(404, 'Key not found in SAM.')
        sam_entry = loads(response.body)
        grains = sam_entry['data']['grains_keys']
        grains['thumbnail'] = sam_entry['data']['thumbnail_key']
        return grains

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def post(self):
        request_as_json = self._load_request_as_json()
        expire = request_as_json.get('expire') or False
        callback_url = request_as_json.get('callback') or None
        callback_verb = request_as_json.get('verb') or 'POST'
        if not request_as_json.get('filename') and not request_as_json.get('doc_link') and not request_as_json.get('metadata'):
            log.msg("POST failed.")
            log.msg("Either filename or doc_link weren't provided.")
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        elif not request_as_json.get('metadata'):
            filename = request_as_json.get('filename') or urlsplit(request_as_json.get('doc_link')).path.split('/')[-1]
        doc_link = None

        if request_as_json.get('doc'):
            doc = request_as_json.get('doc')
            filename = request_as_json.get('filename')
            to_granulate_doc = {"file":doc, 'filename': filename}
            to_granulate_uid = yield self._pre_store_in_sam(to_granulate_doc)
            log.msg("Granulating a doc...")
        # Metadata extraction
        elif request_as_json.get('sam_uid') and request_as_json.get('metadata') and request_as_json.get('type'):
            to_extract_metadata_uid = yield request_as_json.get('sam_uid')
            document_type = request_as_json.get('type')
            response = self.sam.get(key=to_extract_metadata_uid)
            if response.code == '404':
                log.msg("POST failed!")
                log.msg("Couldn't find the key: %s" % to_granulate_uid)
                raise cyclone.web.HTTPError(404, 'Key not found at SAM.')
            elif loads(response.body).get('data').get('metadata_key'):
                print "Metadata for document with key %s was already extracted." % to_extract_metadata_uid
                self.finish(cyclone.web.escape.json_encode({'doc_key':to_extract_metadata_uid}))
                return
            self._enqueue_uid_to_metadata_extraction(to_extract_metadata_uid, document_type,
                                                     callback_url, callback_verb, expire)
            log.msg("Metadata extraction requested for document with key %s." % to_extract_metadata_uid)
            self.finish(cyclone.web.escape.json_encode({'doc_key':to_extract_metadata_uid}))
            return
        # End of metadata extraction
        elif request_as_json.get('sam_uid'):
            to_granulate_uid = yield request_as_json.get('sam_uid')
            response = self.sam.get(key=to_granulate_uid)
            if response.code == '404':
                log.msg("POST failed!")
                log.msg("Couldn't find the key: %s" % to_granulate_uid)
                raise cyclone.web.HTTPError(404, 'Key not found at SAM.')
            log.msg("Granulating from a SAM key...")
        elif request_as_json.get('doc_link'):
            to_granulate_uid = yield self._pre_store_in_sam({"granulated":False})
            doc_link = request_as_json.get('doc_link')
            log.msg("Granulating from web...")
        else:
            log.msg("POST failed!")
            log.msg("Either 'doc', 'sam_uid' or 'doc_link' weren't provided")
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        response = self._enqueue_uid_to_granulate(to_granulate_uid, filename,
                                                  callback_url, callback_verb, doc_link,
                                                  expire)
        self.set_header('Content-Type', 'application/json')
        log.msg("Document sent to the granulation queue.")
        self.finish(cyclone.web.escape.json_encode({'doc_key':to_granulate_uid}))

    def _enqueue_uid_to_granulate(self, uid, filename, callback_url, callback_verb, doc_link, expire):
        try:
            send_task('nsicloudooomanager.tasks.GranulateDoc', args=(self._task_queue, uid, filename, callback_url,
                                                                     callback_verb, doc_link, expire,
                                                                     self.cloudooo_settings, self.sam_settings),
                                                               queue=self._task_queue,
                                                               routing_key=self._task_queue)
        except:
            log.msg('POST failed!')
            log.msg("Couldn't conncet to the queue service.")
            raise cyclone.web.HTTPError(503, 'Queue service unavailable')

    def _enqueue_uid_to_metadata_extraction(self, uid, document_type, callback_url, callback_verb, expire):
        try:
            send_task('nsicloudooomanager.tasks.ExtractMetadata', args=(self._task_queue, uid, document_type,
                                                                     callback_url, callback_verb, expire, self.cloudooo_settings,
                                                                     self.sam_settings),
                                                               queue=self._task_queue,
                                                               routing_key=self._task_queue)
        except:
            log.msg('POST failed!')
            log.msg("Couldn't conncet to the queue service.")
            raise cyclone.web.HTTPError(503, 'Queue service unavailable')

    def _pre_store_in_sam(self, doc):
        response = self.sam.post(value=doc)
        if response.code == '500':
            log.msg("POST failed.")
            log.msg("Error while connecting to SAM.")
            raise cyclone.web.HTTPError(500, 'Error while connecting to SAM.')
        elif response.code == '404':
            log.msg("POST failed.")
            log.msg("Couldn't find any value for the key: %s" % key)
            raise cyclone.web.HTTPError(404, 'Key not found in SAM.' )
        elif response.code == '401':
            log.msg("POST failed.")
            log.msg("SAM user and password didn't match.")
            raise cyclone.web.HTTPError(401, 'SAM user and password not match.')
        return response.resource().key

