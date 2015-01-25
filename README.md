Random proxy middleware for Scrapy (http://scrapy.org/)
=======================================================

Based on aivarsk/scrapy-proxies. Does smart proxy invalidation if it fails certain times and does not recover.

Processes Scrapy requests using a random proxy from list to avoid IP ban and
improve crawling speed.

Get your proxy list from sites like http://www.hidemyass.com/ (copy-paste into text file
and reformat to http://host:port format)

settings.py
-----------

    DOWNLOAD_TIMEOUT = 5

    # Retry many times since proxies often fail
    RETRY_TIMES = 30
    # Retry on most error codes since proxies fail for different reasons
    RETRY_HTTP_CODES = [500, 503, 504, 400, 403, 404, 408]

    DOWNLOADER_MIDDLEWARES = {
        'scrapy.contrib.downloadermiddleware.retry.RetryMiddleware': 90,
        # Fix path to this module
        'yourspider.randomproxy.RandomProxy': 100,
        'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': 110,
    }

    # Proxy list containing entries like
    # http://host1:port
    # http://username:password@host2:port
    # http://host3:port
    # ...
    PROXY_LIST = '/path/to/proxy/list.txt'
    PROXY_MAX_RETRY = 3
    RETRY_PRIORITY_ADJUST = 1


Your spider
-----------

In each callback ensure that proxy /really/ returned your target page by
checking for site logo or some other significant element.
If not - retry request with dont_filter=True

    if not hxs.select('//get/site/logo'):
        yield Request(url=response.url, dont_filter=True)
