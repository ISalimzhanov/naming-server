import grpc

from API import dfservice_pb2, dfservice_pb2_grpc


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = dfservice_pb2_grpc.DFServiceStub(channel)
    response = stub.AddFile(dfservice_pb2.AddRequest(filename='file.txt', data=b'123213123azxxzcsadqwweqwe'))
    print(response)
    response = stub.GetFile(dfservice_pb2.GetRequest(filename='file.txt'))
    print(response)


if __name__ == '__main__':
    run()
