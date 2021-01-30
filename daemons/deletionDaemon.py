from API.storage_service.storageClient import StorageClient
from naming_server.namingServer import NamingServer
import time


class DeletionDaemon:
    def __init__(self, ns: NamingServer):
        self.ns = ns

    def act(self):
        actives = self.ns.get_active_storages()
        for storage in actives:
            chunks = self.ns.to_delete(storage['_id'])
            with StorageClient(storage['connector']) as stub:
                StorageClient.delete(stub, filenames=chunks)
            self.ns.accept_deletions(storage['_id'], chunks)

    def run(self):
        while True:
            time.sleep(60)
            self.act()
