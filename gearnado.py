import sys
import json
import gearman
import tornado
import logging
import chardet
from itertools import islice
from urlparse import urlsplit
from collections import defaultdict
from pyquery import PyQuery as pq
from time import time, sleep
from tornado import ioloop, httpclient
from tornado.options import options, define

# set default encoding to utf-8
reload(sys)
sys.setdefaultencoding("utf-8")

default_user_agent = 'GearNadoBot 0.10'
logger = logging.getLogger()

class JSONDataEncoder(gearman.DataEncoder):

    @classmethod
    def encode(cls, encodable_object):
        return json.dumps(encodable_object)

    @classmethod
    def decode(cls, decodable_string):
        return json.loads(decodable_string)

class JSONGearmanClient(gearman.GearmanClient):
    data_encoder = JSONDataEncoder

class JSONGearmanWorker(gearman.GearmanWorker):
    data_encoder = JSONDataEncoder

class AsyncBatch(object):

    def __init__(self, urls, callback, max_clients=15, batch_timeout_secs=150, request_timeout_secs=15, user_agent=default_user_agent, size_limit=1000000):
        self._io_loop = ioloop.IOLoop()
        self._async_client = httpclient.AsyncHTTPClient(self._io_loop, max_clients=max_clients)

        # ensure url list is uniq
        self._urls = set(urls)
        self._callback = callback
        self._request_timeout_secs = request_timeout_secs
        self._size_limit = size_limit
        self._user_agent = user_agent
        self._num_urls = 0
        self._num_received = 0;

        # timeout and end the loop after timeout_secs
        self._io_loop.add_timeout(time() + batch_timeout_secs, self.timeout)
        self.run()

    def timeout(self):
        logger.warn('AsyncBatch timed out with %s url(s) remaining' % (self._num_urls - self._num_received))
        self._io_loop.stop()

    def queue_url(self, url):
        http_request = httpclient.HTTPRequest(str(url), connect_timeout=5.0, request_timeout=self._request_timeout_secs,
                                                  headers={'User-Agent': self._user_agent});
        self._async_client.fetch(http_request, lambda response: self.callback(url, response))
        self._num_urls += 1

    def callback(self, url, response):
        response_utf8 = None
        if response.body is None:
            logger.debug('empty response received for %s' % url)
        elif (len(response.body) > self._size_limit):
            logger.debug('%s exceeds size limit of %s bytes' % (url, str(self._size_limit)))
        else:
            # force utf-8, we don't much care about pages with other encodings
            response_utf8 = response.body.decode('utf-8', 'replace').encode('utf-8')
            try:
                self._callback(url, response, response_utf8)
            except Exception, e:
                logger.exception('callback exception - parsing %s failed %s' % (url, e))

        self._num_received += 1
        if self._num_received == self._num_urls:
            self._io_loop.stop()

    def run(self):
        host_counts = defaultdict(int)
        for url in self._urls:
            hostname = urlsplit(url).hostname
            host_counts[hostname] += 1
            if host_counts[hostname] < 3:
                self.queue_url(url)
            else:
                logger.debug('skipping %s, already have 2 urls for %s' % (url, hostname))

        if self._num_urls > 0:
            self._io_loop.start()
