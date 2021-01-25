from string import Template
from typing import Optional

import bson
from naming_server.namingServer import NamingServer
import pymongo


class MongoNs(NamingServer):
    def __init__(self, dbname: str, **kwargs):
        super(MongoNs, self).__init__(dbtype='mongo')
        config: dict = kwargs
        if 'host' not in config:
            config['host'] = 'localhost'
        if 'port' not in config:
            config['port'] = 27017
        if 'username' in config:
            try:
                uri = "mongodb://$username:$password@" \
                      "$host:$port/$dbname?authSource=$auth_source"
                uri = Template(uri).substitute(dbname=dbname, **config)
            except KeyError as ke:
                raise ValueError("Some of necessary parameters haven't passed", ke)
        else:
            uri = "mongodb://$host:$port/$dbname"
            uri = Template(uri).substitute(dbname=dbname, **config)
        client = pymongo.MongoClient(uri)
        self.db = client.get_database()

    def add_storage(self, connector: str, status: str, capacity: int) -> bson.ObjectId:
        storages: pymongo.collection.Collection = self.db.storage
        if storages.find_one({'connector': connector}):
            raise ValueError("storage with the same connector already exists")
        storage_id = storages.insert_one(
            {
                "connector": connector,
                "status": status,
                "capacity": capacity,
            }
        ).inserted_id
        return storage_id

    def delete_storage(self, storage_id: bson.ObjectId) -> None:
        storages: pymongo.collection.Collection = self.db.storage
        # toDo delete everything related from stored, deletion_q
        deleted = storages.delete_one({'_id': storage_id}).deleted_count
        if not deleted:
            raise ValueError("there is no such storage")

    def max_capacity_storage(self) -> Optional[dict]:
        storages: pymongo.collection.Collection = self.db.storage
        sorted_ = storages.aggregate(
            [
                {'$sort': {'capacity': -1, '_id': 1}},
                {'$limit': 1},
            ]
        )
        try:
            return [storage for storage in sorted_][0]
        except TypeError:
            return None

    def activate_storage(self, storage_id: bson.ObjectId, new_connector: str) -> None:
        stored: pymongo.collection.Collection = self.db.stored
        deleted = stored.delete_many({'storage.$id': storage_id}).deleted_count
        assert deleted, "is it reasonable"  # TEST
        storages: pymongo.collection.Collection = self.db.storage
        if storages.find_one({'connector': new_connector}):
            raise ValueError("storage with the same connector already exists")
        modified = storages.update_one(
            {'_id': storage_id},
            {'$set': {'status': 'active', 'connector': new_connector}},
        ).modified_count
        if not modified:
            raise ValueError("there is no such storage")

    def deactivate_storage(self, storage_id: bson.ObjectId):
        storages: pymongo.collection.Collection = self.db.storage
        modified = storages.update_one(
            {'_id': storage_id},
            {'$set': {'status': 'inactive'}},
        ).modified_count
        if not modified:
            raise ValueError("there is no such inactive storage")
        stored: pymongo.collection.Collection = self.db.stored
        stored_chunks = [chunk.id for chunk in stored.find({'storage.$id': storage_id})]
        stored.delete_many({'storage.$id': storage_id})
        self.queue_deletions(storage_id, stored_chunks)

    def __chunking(self, file_id: bson.ObjectId, file_size: int, chunk_size: int) -> list:
        chunks: pymongo.collection.Collection = self.db.chunk
        fist_bit = 0
        res = []
        while fist_bit < file_size:
            chunk_id = chunks.insert_one(
                {
                    'file': bson.DBRef('file', file_id),
                    'first_bit': fist_bit,
                    'size': chunk_size,
                }
            ).inserted_id
            res.append(chunk_id)
            fist_bit += chunk_size
        return res

    def add_file(self, size: int, name: str) -> bson.ObjectId:
        files: pymongo.collection.Collection = self.db.file
        if files.find_one({'name': name}):
            raise ValueError("file with the same name already exists")
        file_id = files.insert_one(
            {
                'name': name,
                'size': size,
            }
        ).inserted_id
        self.__chunking(file_id, size, self.chunk_size)
        return file_id

    def __delete_chunks(self, file_id: bson.ObjectId):
        chunks: pymongo.collection.Collection = self.db.chunk
        # toDo add chunks to deletion queue
        deleted_count = chunks.delete_many({'file.$id': file_id}).deleted_count
        assert deleted_count, "there are not chunks of the file"  # TEST

    def delete_file(self, name: str) -> None:
        files: pymongo.collection.Collection = self.db.file
        try:
            file_id = files.find_one({'name': name})['_id']
        except TypeError:
            raise ValueError("file with such name doesn't exists")
        self.__delete_chunks(file_id)
        files.delete_one({'_id': file_id})

    def set_stored(self, storage_id: bson.ObjectId, chunks: list) -> None:
        if not self.db.storage.find_one({'_id': storage_id}):
            raise ValueError("storage with such id doesn't exists")
        # if not self.db.chunks.find_one({'_id': chunk_id}):
        #    raise ValueError("chunk with such id doesn't exists")
        stored: pymongo.collection.Collection = self.db.stored
        stored.insert_many(
            [
                {
                    'chunk': bson.DBRef('chunk', chunk_id),
                    'storage': bson.DBRef('storage', storage_id),
                }
                for chunk_id in chunks
            ]
        )

    def queue_deletions(self, storage_id: bson.ObjectId, chunks: list) -> None:
        if not self.db.storage.find_one({'_id': storage_id}):
            raise ValueError("storage with such id doesn't exists")
        stored = pymongo.collection.Collection = self.db.stored
        stored.delete_many(
            [
                {
                    'chunk.$id': chunk_id,
                    'storage.$id': storage_id,
                }
                for chunk_id in chunks
            ]
        )
        deletion_q: pymongo.collection.Collection = self.db.deletion_q
        deletion_q.insert_many(
            [
                {
                    'storage': bson.DBRef('storage', storage_id),
                    'chunk': bson.DBRef('chunk', chunk_id),
                }
                for chunk_id in chunks
            ]
        )

    def accept_deletions(self, storage_id: bson.ObjectId, chunks: list) -> None:
        deletion_q: pymongo.collection.Collection = self.db.deletion_q
        deleted = deletion_q.delete_many(
            [
                {
                    'storage': bson.DBRef('storage', storage_id),
                    'chunk': bson.DBRef('chunk', chunk_id),
                }
                for chunk_id in chunks
            ]
        ).deleted_count
        if not deleted:
            raise ValueError("nothing were deleted")

    def move_file(self, old_name: str, new_name: str):
        file: pymongo.collection.Collection = self.db.file
        if file.find_one({'name': new_name}):
            raise ValueError("file with such name already exists")
        modified = file.update_one(
            {'name': old_name},
            {'$set': {'name': new_name}}
        ).modified_count
        if not modified:
            raise ValueError("file with such name doesn't exists")

    def not_enough_replicas(self) -> list:
        # toDO
        pass
