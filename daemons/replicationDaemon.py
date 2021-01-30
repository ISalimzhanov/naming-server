from API.storage_service.storageClient import StorageClient
from naming_server.namingServer import NamingServer
import time


def get_data(ns: NamingServer, chunk_id: str) -> bytes:
    storage_id = ns.where_stored(chunk_id)
    if not storage_id:
        with open(f"/temp/{chunk_id}", "rb") as file:
            data = file.read()
    else:
        try:
            with StorageClient(ns.get_connector(storage_id)) as stub:
                data = StorageClient.read(stub, chunk_id)
        except Exception as error:  # in case if Storage is dead now
            print(error)  # TEST
            ns.deactivate_storage(storage_id)
            get_data(ns, chunk_id)
    return data


class ReplicationDaemon:
    def __init__(self, ns: NamingServer, replication_factor: int):
        self.ns = ns
        self.replication_factor = replication_factor

    def act(self):
        to_replicate = self.ns.not_enough_replicas(self.replication_factor)
        for info in to_replicate:
            chunk_id = info['chunk_id']
            times = info['times']
            data = get_data(self.ns, chunk_id)
            for i in range(times):
                storage = self.ns.max_capacity_storage()
                with StorageClient(storage['connector']) as stub:
                    StorageClient.write(stub, chunk_id, data)
                self.ns.set_stored(storage['_id'], chunk_id)

    def run(self):
        while True:
            time.sleep(60)
            self.act()
