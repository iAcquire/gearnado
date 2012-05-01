#!/usr/bin/python
import tornado.ioloop
import tornado.web
import tornado.options
from tornado.options import define, options
from gearnado import JSONGearmanClient, logger

class TweetServer(tornado.web.RequestHandler):
    def initialize(self, gm_client):
        self._gm_client = gm_client

    def post(self):
        data = json.loads(self.get_argument('data', '[]'))
        job_request = self._gm_client.submit_job('tweet_handler', data)
        print job_request

def main():
    define('port', default=8000, help='Port to listen on', type=int)
    define('jobserver', default='localhost:4730', help='Job server host:port', type=str)
    tornado.options.parse_command_line()
    gm_client = JSONGearmanClient([options.jobserver])
    application = tornado.web.Application([(r'/execute.*', TweetServer, dict(gm_client=gm_client))])
    application.listen(options.port, xheaders=True)
    logger.info('Server listening on port ' + str(options.port))
    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()
