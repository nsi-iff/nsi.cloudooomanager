#!/usr/bin/env python
# encoding: utf-8
from os import remove
from os.path import exists
from base64 import decodestring, b64encode
from restfulie import Restfulie
from nsi.multimedia.transform.ogg_converter import OggConverter
from nsi.multimedia.utils import replace_file_extension
from celery.task import task, Task
from celery.execute import send_task

class VideoException(Exception):
    pass

class VideoDownloadException(Exception):
    pass

class VideoConversion(Task):

    def run(self, uid, callback_url, video_link, sam_settings):
        self.callback_url = callback_url
        self.sam = Restfulie.at(sam_settings['url']).auth(*sam_settings['auth']).as_('application/json')
        self.destination_uid = uid
        self.tmp_path = "/tmp/converted"
        video_is_converted = False

        if video_link:
            self._download_video(video_link)
        else:
            response = self._get_from_sam(uid)
            self._original_video = response.data.video
            video_is_converted = response.data.converted

        if not video_is_converted:
            print "Conversion started."
            self._process_video()
            print "Conversion finished."
            if not self.callback_url == None:
                print "Callback task sent."
                send_task('nsivideoconvert.tasks.Callback', args=(callback_url, self.destination_uid), queue='convert',
                          routing_key='convert')
            else:
                print "No callback."
            return self.destination_uid
        else:
            raise VideoException("Video already converted.")

    def _download_video(self, video_link):
        try:
            print "Downloading video from %s" % video_link
            video = Restfulie.at(video_link).get().body
        except Exception:
            raise VideoDownloadException("Could not download the video from %s" % video_link)
        else:
            print "Video downloaded."
        self._original_video = b64encode(video)

    def _process_video(self):
        self._save_video_to_filesystem()
        self._convert_video()
        self._store_in_sam(self.destination_uid, self._converted_video)

    def _save_video_to_filesystem(self):
        video = decodestring(self._original_video)
        video_to_convert = open(self.tmp_path, 'w+')
        video_to_convert.write(video)
        video_to_convert.close()

    def _convert_video(self, destination=None):
        try:
            if not destination:
                destination = (self.tmp_path + '.ogm')
            converter = OggConverter(self.tmp_path, target=destination)
            converter.run()
            converted_video = open(destination or replace_file_extension(self.tmp_path, 'ogm')).read()
        finally:
            files_to_remove = [destination, replace_file_extension(self.tmp_path, 'ogm'), self.tmp_path]
            for file_ in files_to_remove:
                self._remove_if_exists(file_)
            self._converted_video = b64encode(converted_video)

    def _remove_if_exists(self, path):
        if exists(path):
            remove(path)

    def _get_from_sam(self, uid):
        return self.sam.get(key=uid).resource()

    def _store_in_sam(self, uid, video):
        return self.sam.post(key=uid, value=video).resource().key


class Callback(Task):

    max_retries = 3

    def run(self, url, video_uid, **kwargs):
        try:
            print "Sending callback to %s" % url
            response = Restfulie.at(url).as_('application/json').post(key=video_uid, status='Done')
        except Exception, e:
            Callback.retry(exc=e, countdown=10)
        else:
            print "Callback executed."
            print "Response code: %s" % response.code

