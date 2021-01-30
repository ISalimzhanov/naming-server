import abc
import os
from typing import Optional


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
    def get_connector(self, storage_id: str) -> str:
        pass

    @abc.abstractmethod
    def add_storage(self, connector: str, status: str, capacity: int) -> str:
        pass

    @abc.abstractmethod
    def delete_storage(self, storage_id: str) -> None:
        pass

    @abc.abstractmethod
    def get_active_storages(self) -> list:
        pass

    @abc.abstractmethod
    def get_inactive_storages(self) -> list:
        pass

    @abc.abstractmethod
    def activate_storage(self, storage_id: str, new_connector: str) -> None:
        pass

    @abc.abstractmethod
    def deactivate_storage(self, storage_id: str) -> None:
        pass

    @abc.abstractmethod
    def max_capacity_storage(self) -> Optional[dict]:
        pass

    @abc.abstractmethod
    def update_capacity(self, storage_id: str, capacity: int) -> None:
        pass

    @abc.abstractmethod
    def queue_deletions(self, storage_id, chunks: list) -> None:
        pass

    @abc.abstractmethod
    def to_delete(self, storage_id) -> list:
        pass

    @abc.abstractmethod
    def accept_deletions(self, storage_id, chunks) -> None:
        pass

    @abc.abstractmethod
    def _chunking(self, file_id: str, file_size: int) -> list:
        pass

    @abc.abstractmethod
    def add_file(self, size: int, name: str) -> list:
        pass

    @abc.abstractmethod
    def _delete_chunks(self, file_id: str):
        pass

    @abc.abstractmethod
    def delete_file(self, name: str) -> None:
        pass

    @abc.abstractmethod
    def set_stored(self, storage_id, chunks: list) -> None:
        pass

    @abc.abstractmethod
    def chunks_of(self, filename: str) -> list:
        pass

    @abc.abstractmethod
    def where_stored(self, chunk_id: str) -> Optional[str]:
        pass

    @abc.abstractmethod
    def move_file(self, old_name: str, new_name: str) -> None:
        pass

    @abc.abstractmethod
    def not_enough_replicas(self, replication_factor: int) -> list:
        pass
