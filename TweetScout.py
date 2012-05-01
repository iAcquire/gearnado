#!/usr/bin/python
import tornado
import re
from urlparse import urlsplit
from pyquery import PyQuery as pq
from tornado.options import define, options
from gearnado import logger, JSONGearmanWorker, AsyncBatch

define('jobserver', default='localhost:4730', help='Job Server host:port', type=str)

class TweetScout(object):

    def __init__(self):

        self._urldata = dict()
        self._urls = list()
        self._gm_job = None

        self._gm_worker = JSONGearmanWorker([options.jobserver])
        self._gm_worker.register_task('tweet_scout', self.handle_urls)
        self._url_re = re.compile('^http(s)?://(www\.)?twitter\.com/(?!share)(?!home)(?!intent)(#!/)?([a-zA-Z0-9_]{1,15}[^/])(/\w+)*$')

        try:
            logger.info('TweetScout initialized and ready for work');
            self._gm_worker.work()
        except KeyboardInterrupt:
            logger.info('Exiting')
            pass
        except Exception, e:
            logger.error('Exiting - %s' % e)
    
    def handle_urls(self, gm_worker, gm_job):
        self._urldata = dict()
        self._urls = gm_job.data
        self._num_processed = 0
        self._gm_job = gm_job
        logger.info(self._urls)
        AsyncBatch(self._urls, self.parse_response)
        return self._urldata

    def parse_response(self, url, response, response_utf8):
        if response.error:
            logger.info('error: %s' % response.error)

        if response_utf8 is not None:
            logger.info('received %s' % url)
            self._urldata[url] = list()

            try:
                d = pq(response_utf8)
            except ValueError:
               logger.info('Parse of %s failed, trying soup parser...' % url)
               d = pq(response_utf8, parser='soup')

            for link in d('a'):
                href = link.get('href')
                if href is None:
                    continue
                split_url = urlsplit(href)
                if split_url.hostname is None:
                    continue
                url_match = self._url_re.match(href)
                if url_match:
                    logger.info('Twitter User Found: %s' % url_match.group(4))
                    if url_match.group(4) not in self._urldata[url]:
                        self._urldata[url].append(url_match.group(4))

        else:
            logger.debug('Parse of %s failed' % url)
            self._urldata[url] = None

        self._num_processed += 1

if __name__ == '__main__':
    tornado.options.parse_command_line()
    TweetScout()
