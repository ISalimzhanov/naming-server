from naming_server.namingServer import NamingServer


class TrackingDaemon:
    def __init__(self, ns: NamingServer):
        self.ns = ns

    def act(self):
        active_storages = self.ns.get_active_storages()
        for storage_id in active_storages:
            revived = False  # toDo ping storage
            if not revived:
                self.ns.deactivate_storage(storage_id)
