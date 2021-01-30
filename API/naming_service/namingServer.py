from API.naming_service import namingService_pb2, namingService_pb2_grpc
from naming_server.mongo_ns.mongoNs import MongoNs
from naming_server.namingServer import NamingServer


class NamingService(namingService_pb2_grpc.NamingServiceServicer):
    def __init__(self, ns: NamingServer):
        self.ns = ns

    def Register(self, request, context):
        try:
            id_ = self.ns.add_storage(connector=request.connector, status='active', capacity=request.capacity)
            return namingService_pb2.RegistrationReply(success=True, id=str(id_))
        except ValueError as error:
            return namingService_pb2.RegistrationReply(success=False, error=error)

    def Connect(self, request, context):
        try:
            self.ns.activate_storage(request.id, request.connector)
            return namingService_pb2.ConnectionReply(success=True)
        except ValueError as error:
            return namingService_pb2.ConnectionReply(success=False, error=error)


def serve(server, port: int):
    ns = MongoNs(dbname="test", username="user", password="abc123", auth_source="test")
    namingService_pb2_grpc.add_NamingServiceServicer_to_server(NamingService(ns), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()
