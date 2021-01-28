import os

from API import dfservice_pb2, dfservice_pb2_grpc
from concurrent import futures
import logging
import grpc

from naming_server.mongo_ns.mongoNs import MongoNs
from naming_server.namingServer import NamingServer


class DFService(dfservice_pb2_grpc.DFServiceServicer):
    def __init__(self, ns: NamingServer):
        self.ns = ns

    def AddFile(self, request, context):
        try:
            self.ns.add_file(size=len(request.data), name=request.filename)
            return dfservice_pb2.AddReply(success=True)
        except ValueError as err:
            return dfservice_pb2.AddReply(success=False, error=err)

    def DeleteFile(self, request, context):
        try:
            self.ns.delete_file(name=request.filename)
            return dfservice_pb2.DeleteReply(success=True)
        except ValueError as err:
            return dfservice_pb2.DeleteReply(success=False, error=err)

    def GetFile(self, request, context):
        try:
            # toDo ask Storage for a data
            return dfservice_pb2.GetReply(success=True, data=b'Not done yet')
        except ValueError as err:
            return dfservice_pb2.AddReply(success=False, error=err)


def serve():
    os.environ['chunk_size'] = "256"  # toDo add args
    ns = MongoNs(dbname="test", username="user", password="abc123", auth_source="test")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfservice_pb2_grpc.add_DFServiceServicer_to_server(DFService(ns), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
