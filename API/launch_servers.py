import os
from concurrent import futures
import logging
from threading import Thread
import API.dfs_service.dfsServer as dfsServer
import API.naming_service.namingServer as namingServer
import grpc


def launch_server(serve, port) -> tuple:
    """
    :return: thread where server running, Flask's app
    """
    os.environ['chunk_size'] = "256"  # toDo add args
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    thread = Thread(target=serve, args=(server, port))
    thread.start()
    return server, thread


if __name__ == '__main__':
    os.environ['dfs_port'] = '5050'  # temp
    os.environ['naming_port'] = '5051'  # temp
    logging.basicConfig()
    launch_server(dfsServer.serve, int(os.environ['dfs_port']))  # toDo add args
    launch_server(namingServer.serve, int(os.environ['naming_port']))  # toDo add args
