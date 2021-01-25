import abc
import os


class NamingServer(abc.ABC):
    def __new__(cls, *args, **kwargs):
        if not hasattr(NamingServer, '__instance'):
            setattr(NamingServer, '__instance', super(NamingServer, cls).__new__(cls))
        return getattr(NamingServer, '__instance')

    def __init__(self, dbtype: str):
        if hasattr(NamingServer, 'dbtype') and getattr(NamingServer, 'dbtype') != dbtype:
            raise ValueError("Naming Server's database type should not change")
        setattr(NamingServer, 'dbtype', dbtype)
        self.chunk_size = int(os.environ['chunk_size'])

    @abc.abstractmethod
    def add_storage(self, connector: str, status: str, capacity: int):
        pass

    @abc.abstractmethod
    def delete_storage(self, storage_id):
        pass

    @abc.abstractmethod
    def activate_storage(self, storage_id, new_connector: str):
        pass

    @abc.abstractmethod
    def deactivate_storage(self, storage_id):
        pass

    @abc.abstractmethod
    def max_capacity_storage(self):
        pass

    @abc.abstractmethod
    def queue_deletions(self, storage_id, chunks: list):
        pass

    @abc.abstractmethod
    def accept_deletions(self, storage_id, chunks):
        pass

    @abc.abstractmethod
    def add_file(self, data: bytes, name: str):
        pass

    @abc.abstractmethod
    def delete_file(self, name: str):
        pass

    @abc.abstractmethod
    def set_stored(self, storage_id, chunks: list):
        pass

    @abc.abstractmethod
    def move_file(self, old_name: str, new_name: str):
        pass

    @abc.abstractmethod
    def not_enough_replicas(self) -> list:
        pass
