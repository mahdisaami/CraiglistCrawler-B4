import json
from abc import abstractmethod, ABC
from time import sleep

import requests
from bs4 import BeautifulSoup

from config import BASE_LINK
from parser import AdvertisementPageParser
from config import STORAGE_TYPE
from utils import get_cookie
from storage import MongoStorage, FileStorage


class BaseCrawler(ABC):

    def __init__(self):
        self.storage = self.__set_storage()
        # self.cookie = get_cookie()

    @staticmethod
    def __set_storage():
        if STORAGE_TYPE == 'mongo':
            return MongoStorage()
        return FileStorage()

    @abstractmethod
    def start(self, ):
        pass

    @abstractmethod
    def store(self, date, filename = None):
        pass

    @staticmethod
    def get(link):

        try:
            response = requests.get(link) # cookies = self.cookie to request with our cookie
        except requests.HTTPError:
            return None
        return response


class LinkCrawler(BaseCrawler):

    def __init__(self, cities, link = BASE_LINK):
        self.cities = cities
        self.link = link
        super().__init__()

    @staticmethod
    def find_links(html_doc):
        soup = BeautifulSoup(html_doc, 'html.parser')
        links = soup.find_all('li', attrs = {'class': 'cl-static-search-result'})
        adv_links = list()
        for li in links:
            link = li.find('a')
            href = link.get('href')
            adv_links.append(href)
        return adv_links

    def start_crawl_city(self, url):
        crawl = True
        adv_list = list()
        while crawl:
            response = self.get(url)
            if response is None:
                crawl = False
                continue
            links = self.find_links(response.text)
            adv_list.extend(links)
            crawl = False

        return adv_list

    def start(self, store = False):
        adv_links = list()
        for city in self.cities:
            links = self.start_crawl_city(self.link.format(city))
            print(f'{city} total: {len(links)}')
            adv_links.extend(links)
        if store:
            self.store([{'url': li, 'flag': False} for li in adv_links])  # We add flag to see what links we have parsed
        return adv_links

    def store(self, data, *args):
        self.storage.store(data, 'advertisements_links')


class DataCrawler(BaseCrawler):

    def __init__(self):
        super().__init__()
        self.links = self.__load_links()
        self.parser = AdvertisementPageParser()

    def __load_links(self):
        return self.storage.load('advertisements_links', {'flag': False})

    def start(self, store = False):
        for link in self.links:
            sleep(5)
            response = self.get(link['url'])
            data = self.parser.parse(response.text)
            if store:
                self.store(data, data.get('post_id', 'sample'))
            self.storage.update_flag(link)

    def store(self, data, filename):
        self.storage.store(data, 'advertisement_data')
        print(data['post_id'])


class ImageDownloader(BaseCrawler):

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.advertisements = self.__load_advertisements()

    @staticmethod
    def get(link):

        try:
            response = requests.get(link, stream = True)
        except requests.HTTPError:
            return None
        return response

    def __load_advertisements(self):
        return self.storage.load('advertisement_data')

    def start(self, store = True):
        for advertisement in self.advertisements:
            counter = 1
            for img in advertisement['images']:
                response = self.get(img['url'])
                if store:
                    self.store(response, advertisement['post_id'], counter)
                counter += 1

    def store(self, data, adv_id, img_number):
        filename = f'{adv_id}-{img_number}'
        return self.save_to_disk(data, filename )

    @staticmethod
    def save_to_disk(response, filename):
        with open(f'fixtures/images/{filename}.jpg', 'ab') as f:
            for chunk in response.iter_content(chunk_size = 8192):
                if chunk:
                    f.write(chunk)
            # f.write(response.content)
        print(filename)
        return filename
