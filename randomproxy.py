# Copyright (C) 2013 by Aivars Kalvans <aivars.kalvans@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import re
import random
import base64
from scrapy import log

class RandomProxy(object):
    def __init__(self, crawler, stats):
        settings = crawler.settings
        self.crawler = crawler
        self.stats = stats
        self.proxy_list = settings.get('PROXY_LIST')
        self.proxy_max_retry = settings.get('PROXY_MAX_RETRY')
        fin = open(self.proxy_list)

        self.proxies = {}
        self.proxyRetries = {}
        self.failedProxies = {}
        for line in fin.readlines():
            parts = re.match('(\w+://)(\w+:\w+@)?(.+)', line)

            # Cut trailing @
            if parts.group(2):
                user_pass = parts.group(2)[:-1]
            else:
                user_pass = ''

            self.proxies[parts.group(1) + parts.group(3)] = user_pass
            self.proxyRetries[parts.group(1) + parts.group(3)] = 0

        fin.close()

        self.stats.set_value('randomproxy/proxies_provided_num', len(self.proxies))

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler, crawler.stats)

    def process_request(self, request, spider):
        prevProxy = request.meta.get('proxy')
        if prevProxy in self.proxyRetries and request.meta.get('retry_times', 0) > 0:
            self.proxyRetries[prevProxy] += 1
        elif prevProxy in self.proxyRetries:
            self.proxyRetries[prevProxy] -= 0.8
            if self.proxyRetries[prevProxy] < 0:
                self.proxyRetries[prevProxy] = 0

        if prevProxy in self.proxyRetries and self.proxyRetries[prevProxy] > self.proxy_max_retry:
            log.msg('Removing failed proxy <%s>, %d proxies left' % (
                    prevProxy, len(self.proxies)))
            del self.proxies[prevProxy]
            del self.proxyRetries[prevProxy]
            self.stats.inc_value('randomproxy/proxies_failed_num')

        if len(self.proxies) == 0:
            log.msg(format="No proxies left. Exiting",
                    level=log.ERROR)
            self.crawler.engine.close_spider(spider, 'no_proxies_left')
            return

        proxy_address = random.choice(self.proxies.keys())
        proxy_user_pass = self.proxies[proxy_address]

        request.meta['proxy'] = proxy_address
        if proxy_user_pass:
            basic_auth = 'Basic ' + base64.encodestring(proxy_user_pass)
            request.headers['Proxy-Authorization'] = basic_auth