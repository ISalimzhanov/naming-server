from API.storage_service.storageClient import StorageClient
from naming_server.namingServer import NamingServer


class TrackingDaemon:
    def __init__(self, ns: NamingServer):
        self.ns = ns

    def act(self):
        active_storages = self.ns.get_active_storages()
        for storage in active_storages:
            try:
                with StorageClient(storage['connector']) as stub:
                    revived = StorageClient.ping(stub)
                    if not revived:
                        self.ns.deactivate_storage(storage['_id'])
            except Exception:
                self.ns.deactivate_storage(storage['_id'])
