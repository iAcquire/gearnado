gearnado
========

Experimental Distributed Web Crawling with Python + Gearman


Setup Instructions for Ubuntu:

    $ sudo apt-get install git gearman libgearman-dev python-setuptools build-essential libxml2-dev libxslt-dev python-dev

    $ sudo easy_install pyquery gearman tornado

If you are looking to do more than 1024 simultaneous connections on a single machine make sure you edit /etc/security/limits.conf and increase the soft/hard nofile limits.

Clone the Git Repo:

    $ git clone https://github.com/iAcquire/gearnado
    $ cd gearnado

Launch 30 TweetScout workers in one terminal:

    $ for i in `seq 1 30`; do ./TweetScout.py & done

And run the TweetHandler in another:

    $ time ./TweetHandler.py --url_file=python_crawler_urls.txt

