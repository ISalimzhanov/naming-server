from API.storage_service import storageService_pb2, storageService_pb2_grpc

import grpc


class StorageClient:
    def __init__(self, connector: str):
        self.connector = connector

    def __enter__(self):
        self._channel = grpc.insecure_channel(self.connector)
        stub = storageService_pb2_grpc.StorageServiceStub(self._channel)
        return stub

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._channel.close()

    @staticmethod
    def ping(stub) -> bool:
        try:
            response = stub.Ping(storageService_pb2.ControlRequest())
            return response.success
        except Exception:
            return False

    @staticmethod
    def create(stub, filename: str) -> int:
        response = stub.Create(storageService_pb2.UpdateRequest(filename=filename))
        if response.success:
            return response.capacity
        raise ValueError(response.error)

    @staticmethod
    def write(stub, filename: str, data: bytes) -> int:
        response = stub.Write(storageService_pb2.UpdateRequest(filename=filename, data=data))
        if response.success:
            return response.capacity
        raise ValueError(response.error)

    @staticmethod
    def read(stub, filename: str) -> bytes:
        response = stub.Read(storageService_pb2.GetRequest(filename=filename))
        if response.success:
            return response.data
        raise ValueError(response.error)

    @staticmethod
    def delete(stub, filenames: list) -> int:
        response = stub.Delete(storageService_pb2.DeleteRequest(filename=filenames))
        if response.success:
            return response.capacity
        raise ValueError(response.error)

    @staticmethod
    def clear(stub):
        response = stub.Clear(storageService_pb2.ControlRequest())
        if response.success:
            return response.data
        raise ValueError(response.error)
