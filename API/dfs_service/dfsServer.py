from API.dfs_service import dfsService_pb2_grpc, dfsService_pb2
from naming_server.mongo_ns.mongoNs import MongoNs
from naming_server.namingServer import NamingServer


class DFService(dfsService_pb2_grpc.DFServiceServicer):
    def __init__(self, ns: NamingServer):
        self.ns = ns

    def AddFile(self, request, context):
        try:
            self.ns.add_file(size=len(request.data), name=request.filename)
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
            # toDo ask Storage for a data
            return dfsService_pb2.GetReply(success=True, data=b'Not done yet')
        except ValueError as err:
            return dfsService_pb2.GetReply(success=False, error=err)


def serve(server, port: int):
    ns = MongoNs(dbname="test", username="user", password="abc123", auth_source="test")
    dfsService_pb2_grpc.add_DFServiceServicer_to_server(DFService(ns), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()
