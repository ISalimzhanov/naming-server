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

    def get_connector(self, storage_id: str) -> str:
        storages: pymongo.collection.Collection = self.db.storage
        storage_id: bson.ObjectId = bson.ObjectId(storage_id)
        storage = storages.find_one({'_id': storage_id})
        if not storage:
            raise ValueError("there is not such storage")
        return storage['connector']

    def add_storage(self, connector: str, status: str, capacity: int) -> str:
        storages: pymongo.collection.Collection = self.db.storage
        if storages.find_one({'connector': connector}):
            raise ValueError("storage with the same connector already exists")
        storage_id: bson.ObjectId = storages.insert_one(
            {
                "connector": connector,
                "status": status,
                "capacity": capacity,
            }
        ).inserted_id
        return str(storage_id)

    def delete_storage(self, storage_id: str) -> None:
        storage_id: bson.ObjectId = bson.ObjectId(storage_id)
        storages: pymongo.collection.Collection = self.db.storage
        # Delete from stored
        stored: pymongo.collection.Collection = self.db.stored
        stored.delete_many({'storage.$id': storage_id})
        # Delete from deletion q
        deletion_q: pymongo.collection.Collection = self.db.deletion_q
        deletion_q.delete_many({'storage.$id': storage_id})
        deleted = storages.delete_one({'_id': storage_id}).deleted_count
        if not deleted:
            raise ValueError("there is no such storage")

    def get_active_storages(self) -> list:
        storages: pymongo.collection.Collection = self.db.storage
        active = storages.find({'status': 'active'})
        res = [storage for storage in active]
        for r in res:
            r['_id'] = str(r['_id'])
        return res

    def get_inactive_storages(self) -> list:
        storages: pymongo.collection.Collection = self.db.storage
        inactive = storages.find({'status': 'inactive'})
        res = [storage for storage in inactive]
        for r in res:
            r['_id'] = str(r['_id'])
        return res

    def max_capacity_storage(self) -> Optional[dict]:
        storages: pymongo.collection.Collection = self.db.storage
        sorted_ = storages.aggregate(
            [
                {'$sort': {'capacity': -1, '_id': 1}},
                {'$limit': 1},
            ]
        )
        try:
            res: dict = [storage for storage in sorted_][0]
            res['_id'] = str(res['_id'])
            return res
        except TypeError:
            return None

    def update_capacity(self, storage_id: str, capacity: int) -> None:
        storage_id: bson.ObjectId = bson.ObjectId(storage_id)
        storages: pymongo.collection.Collection = self.db.storage
        modified = storages.update_one(
            {'_id': storage_id},
            {'$set': {'capacity': capacity}}
        ).modified_count
        if not modified:
            raise ValueError("there is no such storage")

    def activate_storage(self, storage_id: str, new_connector: str) -> None:
        storage_id: bson.ObjectId = bson.ObjectId(storage_id)
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

    def deactivate_storage(self, storage_id: str) -> None:
        storage_id: bson.ObjectId = bson.ObjectId(storage_id)
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
        self.queue_deletions(str(storage_id), stored_chunks)

    def _chunking(self, file_id: str, file_size: int) -> list:
        file_id: bson.ObjectId = bson.ObjectId(file_id)
        chunks: pymongo.collection.Collection = self.db.chunk
        first_bit = 0
        res = []
        while first_bit < file_size:
            size = min(self.chunk_size, file_size - first_bit)
            chunk_id = chunks.insert_one(
                {
                    'file': bson.DBRef('file', file_id),
                    'first_bit': first_bit,
                    'size': size,
                }
            ).inserted_id
            res.append({'_id': chunk_id, 'first_bit': first_bit, 'size': size})
            first_bit += self.chunk_size
        return res

    def add_file(self, size: int, name: str) -> list:
        files: pymongo.collection.Collection = self.db.file
        if files.find_one({'name': name}):
            raise ValueError("file with the same name already exists")
        file_id: bson.ObjectId = files.insert_one(
            {
                'name': name,
                'size': size,
            }
        ).inserted_id
        return self._chunking(str(file_id), size)

    def _delete_chunks(self, file_id: str):
        file_id: bson.ObjectId = bson.ObjectId(file_id)
        chunks: pymongo.collection.Collection = self.db.chunk
        file_chunks = chunks.find({'file.$id': file_id})
        to_delete = {}
        for chunk in file_chunks:
            chunk_id = str(chunk['_id'])
            storage_id = self.where_stored(chunk_id)
            if storage_id not in to_delete:
                to_delete[storage_id] = [chunk_id]
            else:
                to_delete[storage_id].append(chunk_id)
        for storage_id, storage_chunks in to_delete.items():
            self.queue_deletions(storage_id, storage_chunks)
        deleted_count = chunks.delete_many({'file.$id': file_id}).deleted_count
        assert deleted_count, "there are not chunks of the file"  # TEST

    def delete_file(self, name: str) -> None:
        files: pymongo.collection.Collection = self.db.file
        try:
            file_id: bson.ObjectId = files.find_one({'name': name})['_id']
        except TypeError:
            raise ValueError("file with such name doesn't exists")
        self._delete_chunks(str(file_id))
        files.delete_one({'_id': file_id})

    def set_stored(self, storage_id: str, chunks: list) -> None:
        storage_id: bson.ObjectId = bson.ObjectId(storage_id)
        if not self.db.storage.find_one({'_id': storage_id}):
            raise ValueError("storage with such id doesn't exists")
        stored: pymongo.collection.Collection = self.db.stored
        stored.insert_many(
            [
                {
                    'chunk': bson.DBRef('chunk', bson.ObjectId(chunk_id)),
                    'storage': bson.DBRef('storage', storage_id),
                }
                for chunk_id in chunks
            ]
        )

    def queue_deletions(self, storage_id: str, chunks: list) -> None:
        storage_id: bson.ObjectId = bson.ObjectId(storage_id)
        if not self.db.storage.find_one({'_id': storage_id}):
            raise ValueError("storage with such id doesn't exists")
        stored = pymongo.collection.Collection = self.db.stored
        stored.delete_many(
            [
                {
                    'chunk.$id': bson.ObjectId(chunk_id),
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
                    'chunk': bson.DBRef('chunk', bson.ObjectId(chunk_id)),
                }
                for chunk_id in chunks
            ]
        )

    def to_delete(self, storage_id: str) -> list:
        storage_id: bson.ObjectId = bson.ObjectId(storage_id)
        deletion_q: pymongo.collection.Collection = self.db.deletion_q
        to_delete = deletion_q.find({'storage.$id': storage_id})
        return [str(deletion['chunk'].id) for deletion in to_delete]

    def accept_deletions(self, storage_id: str, chunks: list) -> None:
        storage_id: bson.ObjectId = bson.ObjectId(storage_id)
        deletion_q: pymongo.collection.Collection = self.db.deletion_q
        deleted = deletion_q.delete_many(
            [
                {
                    'storage': bson.DBRef('storage', storage_id),
                    'chunk': bson.DBRef('chunk', bson.ObjectId(chunk_id)),
                }
                for chunk_id in chunks
            ]
        ).deleted_count
        if not deleted:
            raise ValueError("nothing were deleted")

    def move_file(self, old_name: str, new_name: str):
        files: pymongo.collection.Collection = self.db.file
        if files.find_one({'name': new_name}):
            raise ValueError("file with such name already exists")
        modified = files.update_one(
            {'name': old_name},
            {'$set': {'name': new_name}}
        ).modified_count
        if not modified:
            raise ValueError("file with such name doesn't exists")

    def not_enough_replicas(self, replication_factor: int) -> list:
        chunks: pymongo.collection.Collection = self.db.chunk
        stored_at = chunks.aggregate(
            [
                {
                    '$lookup':
                        {
                            'from': 'stored',
                            'localField': '_id',
                            'foreignField': 'chunk.$id',
                            'as': 'stored_at',
                        }
                },
            ]
        )
        res = []
        for chunk_info in stored_at:
            replicated = len(chunk_info['stored_at'])
            if replicated < replication_factor:
                res.append({'chunk_id': str(chunk_info['_id']), 'times': replication_factor - replicated})
        return res

    def where_stored(self, chunk_id: str) -> Optional[str]:
        chunk_id: bson.ObjectId = bson.ObjectId(chunk_id)
        stored: pymongo.collection.Collection = self.db.stored
        stored_at = stored.find_one({'chunk.$id': chunk_id})
        if not stored_at:
            return None
        storage_id = str(stored_at['storage'].id)
        return storage_id

    def chunks_of(self, filename: str) -> list:
        files: pymongo.collection.Collection = self.db.file
        file_id = files.find_one({'name': filename})
        if not file_id:
            raise ValueError("there is not such file")
        chunks: pymongo.collection.Collection = self.db.chunk
        chunks = chunks.find(
            {'file.$id': file_id}
        )
        res = [chunk for chunk in chunks]
        res.sort(key=lambda x: x['first_bit'])
        return [str(chunk['_id']) for chunk in res]
