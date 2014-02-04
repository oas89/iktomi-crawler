import re
import sys
import logging
import urlparse
import multiprocessing

from lxml import html

from webob import Request
from webob.exc import HTTPError

from iktomi.cli.base import Cli
from iktomi.web.reverse import Reverse
from iktomi.utils.storage import VersionedStorage


__all__ = ['Crawler']


class String(unicode):
    parent = ''


class CrawlingManager(object):

    def __init__(self, app, env_class, fast=True):
        self.app = app
        self.root = Reverse.from_handler(app)
        self.env_class = env_class
        self.fast = fast

        self._manager = multiprocessing.Manager()
        self._pending = self._manager.JoinableQueue()
        self._visited = self._manager.dict()
        self._lock = self._manager.Lock()

    def fetch(self, url):
        request = Request.blank(url)
        env = VersionedStorage(self.env_class, request, self.root)
        data = VersionedStorage()
        return self.app(env, data)

    def join(self):
        self._pending.join()

    def put(self, urls):
        with self._lock:
            if not isinstance(urls, list):
                urls = [urls]
            for url in urls:
                if url not in self._visited:
                    if self.fast:
                        url = re.sub(r'\d+', '{}', url)
                    self._visited[url] = None
                    self._pending.put(url)

    def get(self):
        return self._pending.get()

    def done(self, url, code):
        with self._lock:
            self._visited[url] = code
            self._pending.task_done()
            self._echo(len(self._visited))

    def _echo(self, count):
        sys.stdout.write('\rVisited \33[92m%s\33[0m links' % count)
        sys.stdout.flush()


class Worker(multiprocessing.Process):

    allowed_codes = [200]
    allowed_types = ['text/html']

    def __init__(self, manager):
        super(Worker, self).__init__()
        self.manager = manager

    def urls(self, content, parent):
        try:
            tree = html.fromstring(content)
            tree.make_links_absolute(parent)
        except:
            return
        for link in tree.xpath('//a'):
            if link.attrib.get('href'):
                url = link.attrib['href']
                parts = urlparse.urlparse(url)
                if not parts.scheme and not parts.netloc:
                    #yield urlunparse((parts, None, parts.path, None, parts.query, None))
                    s = String(url.split('#')[0])
                    s.parent = parent
                    yield s

    def process(self, url):
        try:
            response = self.manager.fetch(url)
            if response.content_type in self.allowed_types:
                urls = [new_url for new_url in self.urls(response.body, url)]
                self.manager.put(urls)
            return response.status_code
        except HTTPError as e:
            return e.code

    def run(self):
        while True:
            url = self.manager.get()
            try:
                code = self.process(url)
            except Exception as e:
                code = e
            finally:
                self.manager.done(url, code)


class Crawler(Cli):
    def __init__(self, app, env_class):
        self.app = app
        self.env_class = env_class

    def command_run(self, fast=False, url=None, worker_count=None):
        logging.basicConfig(level=logging.CRITICAL)
        logging.disable(logging.CRITICAL)
        
        fast = bool(fast)
        url = url or '/'
        worker_count = worker_count or 2 * multiprocessing.cpu_count()
        print 'Crawling using %s worker(s) starting from %s' % (worker_count, url)

        manager = CrawlingManager(self.app, self.env_class, fast=fast)

        workers = [Worker(manager) for i in range(worker_count)]

        for worker in workers:
            worker.start()

        manager.put(String(url))

        manager.join()

        for worker in workers:
            worker.terminate()

        errors = {url: code
                  for url, code in dict(manager._visited).iteritems()
                  if code != 200}

        print 'Found %s errors:' % len(errors)

        for url, code in errors.iteritems():
            print 'from \33[93m%s\33[0m to \33[93m%s\33[0m => \33[91m%s\33[0m' % (url.parent, url, code)
