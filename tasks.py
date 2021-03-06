
import os
import sys
import cgi
import asyncio
import aiohttp
import tempfile
import mimetypes
import tarfile
import logging


from typing import Union, Generator
from collections import namedtuple, deque
from urllib.parse import urlparse, urlunparse

from bs4 import BeautifulSoup
from celery import Celery


app = Celery(__name__, backend='rpc://', broker='amqp://parse:123456@rabbitmq/')

BASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
CONCURRENCY_LIMIT = 5
MAX_LEVEL = 2
TMP_DIR = os.path.join(BASE_PATH, 'tmp')
STATIC_PATH = os.path.join(BASE_PATH, 'static')
STATIC_BASE = 'http://localhost:5000/static/'

DownloadJob = namedtuple('DownloadJob', ['url', 'level'])
DownloadResult = namedtuple('DownloadResult', ['dj', 'data', 'mime', 'options'])

logger = logging.getLogger(__name__)


def extract(dr: DownloadResult) -> Generator[DownloadJob, None, None]:

    if dr.mime != 'text/html':
        return

    base_url = urlparse(dr.dj.url)

    def prepare_link(link) -> Union[DownloadJob, None]:
        if not link:
            return
        parsed_link = urlparse(link)
        if not parsed_link.netloc:
            u = [base_url.scheme, base_url.netloc] + list(parsed_link)[2:]
            try:
                return DownloadJob(urlunparse(u), dr.dj.level + 1)
            except:
                print('error on url "{}", "{}"'.format(link, u))
        if parsed_link.netloc == base_url.netloc:
            return DownloadJob(parsed_link.geturl(), dr.dj.level + 1)

    soup = BeautifulSoup(dr.data, 'html.parser')
    links = [
        ('a', 'href'),
        ('link', 'href'),
        ('script', 'src'),
        ('img', 'src')
    ]
    for name, attr in links:
        for el in soup.find_all(name):
            href = prepare_link(el.attrs.get(attr, None))
            if href:
                yield href


async def fetch(dj: DownloadJob) -> Union[DownloadResult, None]:
    async with aiohttp.ClientSession() as session:
        logger.info('start {}'.format(dj.url))
        async with session.get(dj.url) as resp:
            if resp.status == 200:
                content_type = resp.headers.get('content-type')
                mime, options = cgi.parse_header(content_type)
                logger.info('done {}'.format(dj.url))
                if mime.startswith('text'):
                    data = await resp.text()
                else:
                    data = await resp.read()
                return DownloadResult(dj, data, mime, options)


def save_data(dr: DownloadResult, path: str):
    parsed_link = urlparse(dr.dj.url)
    store_path = os.path.join(path, parsed_link.netloc, *parsed_link.path.split('/'))

    if store_path.endswith('/'):
        store_path = os.path.join(store_path, 'index.html')
    else:
        _, ext = os.path.splitext(store_path)
        if not ext:
            ext = mimetypes.guess_extension(dr.mime)
            if ext is None:
                ext = '.bin'
            store_path = os.path.join(store_path, 'index{}'.format(ext))

    if os.path.exists(store_path):
        return

    store_dir = os.path.dirname(store_path)
    if not os.path.exists(store_dir):
        os.makedirs(store_dir)
    if isinstance(dr.data, str):
        mode = 'w'
    else:
        mode = 'wb'
    with open(store_path, mode) as f:
        f.write(dr.data)


def make_tar(tar_file: str, path: str):

    def filter1(tarinfo: tarfile.TarInfo):
        tarinfo.name = tarinfo.name[len(path):]
        return tarinfo

    tar = tarfile.open(tar_file, mode='w:gz')

    for root, dirs, files in os.walk(path):
        for file in files:
            tar.add(os.path.join(root, file), filter=filter1)

    tar.close()


async def download(url: str, path: str, id: str):

    links = deque()
    dj = DownloadJob(url, 1)
    tasks = [asyncio.create_task(fetch(dj))]
    visited = [dj.url]
    while links or tasks:
        if tasks:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for f in done:
                result = await f
                if isinstance(result, DownloadResult):
                    if result.mime == 'text/html':
                        links.extend(list(extract(result)))
                    save_data(result, path)

        tasks = [task for task in tasks if not task.done()]

        if len(tasks) >= CONCURRENCY_LIMIT:
            continue

        for i in range(CONCURRENCY_LIMIT - len(tasks)):
            try:
                dj = links.popleft()
                if dj.url not in visited and dj.level <= MAX_LEVEL:
                    tasks.append(asyncio.create_task(fetch(dj)))
                    visited.append(dj.url)
            except IndexError:
                break

    # make archive
    parsed_link = urlparse(url)
    name = '{}-{}.tar.gz'.format(parsed_link.netloc, id)
    tar_file = os.path.join(STATIC_PATH, name)
    make_tar(tar_file, path)
    return STATIC_BASE + name


@app.task
def do_parse(url):
    logger.info('start crawling of "{}" in task {}'.format(url, do_parse.request.id))
    path = tempfile.mkdtemp(dir=TMP_DIR)
    logger.info('files will be placed into "{}"'.format(path))
    return asyncio.run(download(url, path, do_parse.request.id))


if __name__ == '__main__':
    asyncio.run(download('http://quotes.toscrape.com/', '/tmp/parse1', '001'))
    make_tar('/tmp/test.tar.gz', '/tmp/parse1')

