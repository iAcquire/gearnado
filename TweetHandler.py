#!/usr/bin/python
import tornado
import time
from collections import defaultdict
from tornado.options import define, options
from gearnado import logger, JSONGearmanWorker, JSONGearmanClient
from progressbar import ProgressBar

define('jobserver', default='localhost:4730', help='Job Server host:port', type=str)
define('url_file', help='Filename containing the list of URLs to process', type=str)
define('chunk_size', default=15, help='Number of URLs to send to each TweetHandler worker', type=int)

class TweetHandler(object):

    def __init__(self, filename):

        self._gm_client = JSONGearmanClient([options.jobserver])

        try:
            urls = list()
            for line in open(filename, 'r'):
                urls.append(line.strip())

            url_chunks = [urls[i : i + options.chunk_size] for i in range(0, len(urls), options.chunk_size)]

            jobs = list()
            for url_chunk in url_chunks:
                jobs.append({'task': 'tweet_scout', 'data': url_chunk});

            submitted_job_requests = self._gm_client.submit_multiple_jobs(jobs, background=False, wait_until_complete=False)
            job_count = len(submitted_job_requests)
            complete_count = 0
            p = ProgressBar(maxval=job_count).start()
            while complete_count != job_count:
                try:
                    self._gm_client.wait_until_jobs_completed(submitted_job_requests, poll_timeout=1)
                    self._gm_client.get_job_statuses(submitted_job_requests, poll_timeout=100)
                except:
                    pass
                complete_count = 0
                for job in submitted_job_requests:
                    if job.complete is True:
                        complete_count += 1
                p.update(complete_count)

            count = 0
            twitter_user_counts = defaultdict(int)
            for job in submitted_job_requests:
                for url, twitter_users in job.result.iteritems():
                    logger.debug('%s: %s' % (url, twitter_users))
                    if twitter_users is not None:
                        count += 1
                        for twitter_user in twitter_users:
                            twitter_user_counts[twitter_user] += 1

            print '\nFound %d Twitter users in %d successfully parsed pages:' % (len(twitter_user_counts), count)
            for user in sorted(twitter_user_counts, key=twitter_user_counts.get, reverse=True):
                print '%s,%d' % (user, twitter_user_counts[user])

        except KeyboardInterrupt:
            logger.info('Exiting')
            pass
        except Exception, e:
            logger.exception('Exiting - %s' % e)
    
if __name__ == '__main__':
    try:
        tornado.options.parse_command_line()
        TweetHandler(options.url_file)
    except Exception, e:
        pass
