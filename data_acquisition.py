import datetime
import os.path
from abc import ABC, abstractmethod
from datetime import datetime
from urllib import request
from zipfile import ZipFile

import kaggle
from sodapy import Socrata
import logging

from sys import argv


logging.basicConfig(level=logging.ERROR)


class DataSet(ABC):

    @abstractmethod
    def download(self):
        pass

    @abstractmethod
    def get_last_mod_date(self):
        pass


class CatalogDataSet(DataSet):

    def __init__(self, filename, domain, _id, url):
        self.filename = filename
        self._id = _id
        self.domain = domain
        self.url = url

    def download(self):
        request.urlretrieve(self.url, FILES_LOCATION + self.filename)

        return True

    def get_last_mod_date(self):
        with Socrata(self.domain, None) as client:
            metadata = client.get_metadata(self._id)

        return datetime.fromtimestamp(metadata['viewLastModified'])


class KaggleDataSet(DataSet):

    def __init__(self, filename, url):
        self.filename = filename
        self.url = url

    def download(self):
        print('Downloading ' + self.filename + "...")
        api = kaggle.KaggleApi()
        api.authenticate()

        api.dataset_download_files(self.url, path=FILES_LOCATION)
        zf = ZipFile(self.url.split('/')[-1] + '.zip')
        zf.extractall()
        zf.close()

        return True

    def get_last_mod_date(self):
        api = kaggle.KaggleApi()
        api.authenticate()

        return api.dataset_view(self.url).lastUpdated


class DataSetManager(object):

    def __init__(self, dataset: DataSet):
        self.dataset = dataset

    def download(self):
        filename = self.dataset.filename
        print('Downloading ' + filename + '...')
        self.dataset.download()
        print('Done downloading ' + filename + '.')

    def remote_dataset_updated(self):
        path = FILES_LOCATION + self.dataset.filename
        last_modified_date = datetime.fromtimestamp(os.path.getmtime(path))

        return last_modified_date < self.dataset.get_last_mod_date()


def download_all(datasets):
    for d in datasets:
        m = DataSetManager(d)
        m.download()


def update_all(datasets):
    for d in datasets:
        m = DataSetManager(d)
        filename = d.filename
        if m.remote_dataset_updated():
            print('Remote file updated. Downloading file ' + filename + '...')
            m.download()
        else:
            print('File ' + filename + ' up to date.')


if __name__ == '__main__':

    mode = argv[1] if len(argv) > 1 else None  # allowed values: init - download all files, NONE - update all files

    FILES_LOCATION = './data/'

    CRIMES_FILENAME = 'crimes_data.json'
    CRIMES_DOMAIN = 'data.lacity.org'
    CRIMES_ID = '63jg-8b9z'
    CRIMES_URL = 'https://data.lacity.org/api/views/63jg-8b9z/rows.json?accessType=DOWNLOAD'

    COLLISIONS_FILENAME = 'collisions_data.xml'
    COLLISIONS_DOMAIN = 'data.lacity.org'
    COLLISIONS_ID = 'd5tf-ez2w'
    COLLISIONS_URL = 'https://data.lacity.org/api/views/d5tf-ez2w/rows.xml?accessType=DOWNLOAD'

    WEATHER_FILENAME = 'weather.csv'
    WEATHER_URL = 'selfishgene/historical-hourly-weather-data'

    DATASETS = [
        CatalogDataSet(CRIMES_FILENAME, CRIMES_DOMAIN, CRIMES_ID, CRIMES_URL),
        CatalogDataSet(COLLISIONS_FILENAME, COLLISIONS_DOMAIN, COLLISIONS_ID, COLLISIONS_URL),
        KaggleDataSet('pressure.csv', WEATHER_URL)
    ]

    if mode == 'init':
        download_all(DATASETS)
    else:
        update_all(DATASETS)
