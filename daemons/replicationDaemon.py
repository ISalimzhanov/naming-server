from API.storage_service.storageClient import StorageClient
from naming_server.namingServer import NamingServer
import time


class ReplicationDaemon:
    def __init__(self, ns: NamingServer, replication_factor: int):
        self.ns = ns
        self.replication_factor = replication_factor

    def act(self):
        to_replicate = self.ns.not_enough_replicas(self.replication_factor)
        for info in to_replicate:
            chunk_id = info['chunk_id']
            times = info['times']
            data = b''  # toDo get chunk data
            for i in range(times):
                storage = self.ns.max_capacity_storage()
                with StorageClient(storage['connector']) as stub:
                    StorageClient.write(stub, chunk_id, data)
                self.ns.set_stored(storage['_id'], chunk_id)

    def run(self):
        while True:
            time.sleep(60)
            self.act()
