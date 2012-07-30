#!/usr/bin/env python
# encoding: utf-8
import sys
import traceback
from os import remove
from os.path import exists
from base64 import decodestring, b64encode
from restfulie import Restfulie
from celery.task import Task
from celery.execute import send_task

from pickle import dumps
from json import loads

from nsi.granulate.GranulateOffice import GranulateOffice
from nsi.granulate.FileUtils import File


class DocumentException(Exception):
    pass

class DocumentDownloadException(DocumentException):
    pass

class GranulateDoc(Task):

    def run(self, uid, filename, callback_url, callback_verb, doc_link, cloudooo_settings, sam_settings):
        self._logger = GranulateDoc.get_logger()
        self._callback_url = callback_url
        self._callback_verb = callback_verb.lower()
        self._cloudooo_settings = cloudooo_settings
        self._sam = Restfulie.at(sam_settings['url']).auth(*sam_settings['auth']).as_('application/json')
        self._doc_uid = uid
        self._filename = filename
        # self._thumbnail_key = None
        doc_is_granulated = False

        if doc_link:
            # link to document
            self._download_doc(doc_link)
        else:
            # when the worker receives a SAM uid
            response = loads(self._get_from_sam(uid).body)
            self._original_doc = response["data"]["doc"]
            self._old_data = response["data"]
            if self._old_data.has_key("granulated"):
                del self._old_data["granulated"]
            doc_is_granulated = response.get('data').get('granulated')

        if not doc_is_granulated:
            try:
                print "Granulation started."
                self._process_doc()
                del self._original_doc
                if hasattr(self, '_old_data'):
                    del self._old_data
                print "Granulation finished."
            except Exception, e:
                print "Error in the granulation."
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=4, file=sys.stdout)
                self._send_fail_callback_task()
            else:
                if not self._callback_url == None:
                    self._send_callback_task()
                else:
                    print "No callback."
                return self._doc_uid
        else:
            raise DocumentException("Document already granulated.")

    def _send_callback_task(self):
        send_task('nsicloudooomanager.tasks.Callback',
                  args=(self._callback_url, self._callback_verb, self._doc_uid, self._grains_keys, self._thumbnail_key),
                  queue='cloudooo', routing_key='cloudooo')
        print "Callback task sent."

    def _send_fail_callback_task(self):
        send_task('nsicloudooomanager.tasks.FailCallback',
                  args=(self._callback_url, self._callback_verb, self._doc_uid),
                  queue='cloudooo', routing_key='cloudooo')
        print "Fail callback task sent."

    def _download_doc(self, doc_link):
        try:
            print "Downloading document from %s" % doc_link
            document = Restfulie.at(doc_link).get().body
        except Exception:
            raise DocumentDownloadException("Could not download the document from %s" % doc_link)
        else:
            print "Document downloaded."
        self._original_doc = b64encode(document)

    def _process_doc(self):
        self._granulate_doc()
        new_doc = {
                    'doc': self._original_doc, 'granulated':True, 'grains_keys':self._grains_keys, 
                    'thumbnail_key':self._thumbnail_key
                  }
        if hasattr(self, '_old_data'):
            new_doc.update(self._old_data)
        self._sam.post(key=self._doc_uid, value=new_doc)

    def _granulate_doc(self):
        doc = File(self._filename, decodestring(self._original_doc))
        print "CloudOOO server: %s" % self._cloudooo_settings['url']
        granulate = GranulateOffice(doc, self._cloudooo_settings['url'])
        grains = granulate.granulate()
        grains_keys = {'images':[], 'files':[]}
        encoded_images = []
        encoded_files = []
        print grains.keys()
        if grains.has_key('image_list'):
            for image in grains['image_list']:
                encoded_image = b64encode(image.getContent().getvalue())
                image_row = {'file': encoded_image, 'filename': image.getId()}
                image_key = self._sam.put(value=image_row).resource().key
                grains_keys['images'].append(image_key)
        if grains.has_key('file_list'):
            for file_ in grains['file_list']:
                encoded_file = b64encode(file_.getContent().getvalue())
                file_row = {'file': encoded_file, 'filename': file_.getId()}
                file_key = self._sam.put(value=file_row).resource().key
                grains_keys['files'].append(file_key)
        if grains.has_key('thumbnail'):
            encoded_thumbnail = b64encode(grains['thumbnail'].getvalue())
            thumbnail_row = {'file': encoded_thumbnail}
            thumbnail_key = self._sam.put(value=thumbnail_row).resource().key
            self._thumbnail_key = thumbnail_key
        print "Document granulated into %d image(s) and %d file(s)." % \
              (len(grains_keys['images']), len(grains_keys['files']))
        self._grains_keys = grains_keys
        del grains

    def _get_from_sam(self, uid):
        return self._sam.get(key=uid)


class Callback(Task):

    max_retries = 3

    def run(self, url, verb, doc_uid, grains_keys, thumbnail_key, **kwargs):
        try:
            print "Sending callback to %s" % url
            restfulie = Restfulie.at(url).as_('application/json')
            response = getattr(restfulie, verb.lower())(doc_key=doc_uid, grains_keys=grains_keys, 
                                                        thumbnail_key=thumbnail_key, done=True)
        except Exception, e:
            Callback.retry(exc=e, countdown=10)
        else:
            print "Callback executed."
            print "Response code: %s" % response.code

class FailCallback(Callback):

    def run(self, url, verb, doc_uid, **kwargs):
        try:
            print "Sending fail callback to %s" % url
            restfulie = Restfulie.at(url).as_('application/json')
            response = getattr(restfulie, verb.lower())(doc_key=doc_uid, done=False, error=True)
        except Exception, e:
            FailCallback.retry(exc=e, countdown=10)
        else:
            print "Fail Callback executed."
            print "Response code: %s" % response.code
