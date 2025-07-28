import json
from abc import ABC, abstractmethod

from mongo import MongoDatabase


class StorageAbstract(ABC):

    @abstractmethod
    def store(self, data, *args):
        pass

    @abstractmethod
    def load(self):
        pass


class MongoStorage(StorageAbstract):

    def __init__(self):
        self.mongo = MongoDatabase()

    def store(self, data, collection, *args):
        coll = getattr(self.mongo.database, collection)
        if isinstance(data, list) and len(data) > 1:
            coll.insert_many(data)
        else:
            coll.insert_one(data)

    def load(self, collection_name, filter_data=None):
        coll = self.mongo.database.get_collection(collection_name)
        if filter_data is not None:
            data = coll.find(filter_data)
        else:
            data = coll.find()
        return data

    def update_flag(self, url):
        self.mongo.database.advertisements_links.find_one_and_update(
            {'_id': url['_id']},
            {'$set': {'flag': True}}
        )


class FileStorage(StorageAbstract):

    def store(self, data, filename, *args):
        filename = filename + '-' + data['post_id']
        with open(f'fixtures/adv/{filename}.json', 'w') as f:
            f.write(json.dumps(data))
        print(f'fixtures/adv/{filename}.json')

    def load(self):
        with open('fixtures/adv/advertisements_links.json', 'r') as f:
            links = json.loads(f.read())
        return links

    def update_flag(self):
        pass
