from API.dfs_service import dfsService_pb2_grpc, dfsService_pb2
from API.storage_service.storageClient import StorageClient
from daemons.replicationDaemon import get_data
from naming_server.mongo_ns.mongoNs import MongoNs
from naming_server.namingServer import NamingServer


class DFService(dfsService_pb2_grpc.DFServiceServicer):
    def __init__(self, ns: NamingServer):
        self.ns = ns

    def AddFile(self, request, context):
        try:
            chunks = self.ns.add_file(size=len(request.data), name=request.filename)
            for chunk in chunks:
                with open(f"/temp/{chunk['_id']}", "wb") as file:
                    first_bit = chunk['first_bit']
                    last_bit = first_bit + chunk['size'] - 1
                    file.write(request.data[first_bit: last_bit + 1])
            return dfsService_pb2.UpdateReply(success=True)
        except ValueError as err:
            return dfsService_pb2.UpdateReply(success=False, error=err)

    def DeleteFile(self, request, context):
        try:
            self.ns.delete_file(name=request.filename)
            return dfsService_pb2.UpdateReply(success=True)
        except ValueError as err:
            return dfsService_pb2.UpdateReply(success=False, error=err)

    def GetFile(self, request, context):
        try:
            chunks = self.ns.chunks_of(filename=request.filename)
            data: bytes = b''
            for chunk_id in chunks:
                data += get_data(self.ns, chunk_id)
            return dfsService_pb2.GetReply(success=True, data=data)
        except ValueError as err:
            return dfsService_pb2.GetReply(success=False, error=err)


def serve(server, port: int):
    ns = MongoNs(dbname="test", username="user", password="abc123", auth_source="test")
    dfsService_pb2_grpc.add_DFServiceServicer_to_server(DFService(ns), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()
