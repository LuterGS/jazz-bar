import grpc
import threading
import logging
from concurrent import futures
from service import NodeService

from data_structure import TableEntry

from utils import generate_hash

from protos.output import chord_pb2
from protos.output import chord_pb2_grpc

class Node:
    """
    - id : int
    - host : string
    - port : string
    """
    pass

class DataTable(TableEntry):
    pass

class FingerTable(TableEntry):
    pass


class ChordNode(Node):
    """
    id : int
    host : string
    port : string

    service : Service
    server : grpc.server

    finger table과 data table은 별도의 class로 관리
    해당 class는 자동 정렬 기능이 있는 list를 담고 있음
    predecessor: Node

    """
    def __init__(self, host, port):
        self.id = generate_hash(host, port)
        self.host = host
        self.port = port
        self.server = None
        self.serve()

    # TODO : join 함수 구현 (우선순위 높음)
    def join_cluster(self, join_address):
        logging.info(f'join from {self.host}:{self.port} to {join_address}')
        with grpc.insecure_channel(join_address) as channel:
            stub = chord_pb2_grpc.NodeStub(channel)
            response = stub.SayHello(chord_pb2.HelloRequest(name=join_address, age=15))
        logging.info(f'{self.host}:{self.port} received {response.message}')

    # TODO : Get/Set/Remove/Join에 대한 핸들링 추가 및 프로토콜 결정 (우선순위 높음)
    def command_handler(self, command):
        commands = command.split()

        if commands[0] == 'get':
            pass
        elif commands[0] == 'set':
            pass
        elif commands[0] == 'remove':
            pass
        elif commands[0] == 'join':
            self.join_cluster(commands[1])

    def listen_command(self):
        try:
            while True:
                command = input("> ")
                self.command_handler(command)
        except KeyboardInterrupt:
            print('Terminated By User')
            self.server.stop(0)

    def serve(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        chord_pb2_grpc.add_NodeServicer_to_server(NodeService(), self.server)

        address = self.host+":"+self.port
        self.server.add_insecure_port(address)
        self.server.start()

        logging.info(f'ChordServer is listening on {self.host}:{self.port}')
        # self.server.wait_for_termination()

        # input thread
        threading.Thread(target=self.listen_command()).start()
