#!/usr/bin/env python
# encoding: utf-8
from os import remove
from os.path import exists
from base64 import decodestring, b64encode
from restfulie import Restfulie
from celery.task import task, Task
from celery.execute import send_task

from pickle import dumps

from nsi.granulate.GranulateOffice import GranulateOffice
from nsi.granulate.FileUtils import File


class DocumentException(Exception):
    pass

class DocumentDownloadException(DocumentException):
    pass

class GranulateDoc(Task):

    def run(self, uid, filename, callback_url, callback_verb, doc_link,\
                                            cloudooo_settings, sam_settings):
        self.callback_url = callback_url
        self.callback_verb = callback_verb.lower()
        self.cloudooo_settings = cloudooo_settings
        self.sam = Restfulie.at(sam_settings['url']).auth(*sam_settings['auth']).as_('application/json')
        self.destination_uid = uid
        self.filename = filename
        doc_is_granulated = False

        if doc_link:
            self._download_doc(doc_link)
        else:
            response = self._get_from_sam(uid)
            self._original_doc = response.data.doc
            doc_is_granulated = response.data.granulated

        if not doc_is_granulated:
            print "Granulation started."
            self._process_doc()
            print "Granulation finished."
            if not self.callback_url == None:
                print "Callback task sent."
                send_task('nsi.cloudooomanager.tasks.Callback',
                            args=(callback_url, callback_verb, self.destination_uid),
                            queue='cloudooo',
                            routing_key='cloudooo')
            else:
                print "No callback."
            return self.destination_uid
        else:
            raise DocumentException("Document already granulated.")

    def _download_doc(self, doc_link):
        try:
            print "Downloading document from %s" % doc_link
            document = Restfulie.at(doc_link).get().body
        except Exception:
            raise DocumentDownloadException("Could not download the document from %s" % video_link)
        else:
            print "Document downloaded."
        self._original_doc = b64encode(document)

    def _process_doc(self):
        self._granulate_doc()
        self._store_in_sam(self.destination_uid, self._grains)

    def _granulate_doc(self):
        doc = File(self.filename, decodestring(self._original_doc))
        print self.cloudooo_settings['url']
        granulate = GranulateOffice(doc, self.cloudooo_settings['url'])
        grains = granulate.granulate()
        encoded_images = [b64encode(dumps(image)) for image in grains['image_list']]
        encoded_files = [b64encode(dumps(file_)) for file_ in grains['file_list']]

        self._grains = {'images':encoded_images, 'files':encoded_files}

    def _get_from_sam(self, uid):
        return self.sam.get(key=uid).resource()

    def _store_in_sam(self, uid, video):
        return self.sam.post(key=uid, value=video).resource().key


class Callback(Task):

    max_retries = 3

    def run(self, url, verb, video_uid, **kwargs):
        try:
            print "Sending callback to %s" % url
            restfulie = Restfulie.at(url).as_('application/json')
            response = getattr(restfulie, verb.lower())(key=video_uid, status='Done')
        except Exception, e:
            Callback.retry(exc=e, countdown=10)
        else:
            print "Callback executed."
            print "Response code: %s" % response.code

