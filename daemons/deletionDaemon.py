from naming_server.namingServer import NamingServer
import time


class DeletionDaemon:
    def __init__(self, ns: NamingServer):
        self.ns = ns

    def act(self):
        actives = self.ns.get_active_storages()
        for storage_id in actives:
            chunks = self.ns.to_delete(storage_id)
            # toDo sent to Storage Server chunks to delete
            self.ns.accept_deletions(storage_id, chunks)

    def run(self):
        while True:
            time.sleep(60)
            self.act()
