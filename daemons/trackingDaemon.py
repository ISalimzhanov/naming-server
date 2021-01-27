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
        inactive_storages = self.ns.get_inactive_storages()
        for storage_id in inactive_storages:
            revived, connector = (False, '')  # toDo ping storage
            if revived:
                self.ns.activate_storage(storage_id, connector)
