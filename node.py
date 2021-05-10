import grpc
import threading
from concurrent import futures
from service import NodeService

from utils import generate_hash

from protos.output import chord_pb2
from protos.output import chord_pb2_grpc

class Node:
    # id : int
    # host : string
    # port : string
    pass

class FingerTableEntry:
    # start : int
    # sucessor : Node
    pass

class ChordNode(Node):
    # id : int
    # host : string
    # port : string

    # service : Service
    # server : grpc.server

    # finger_table : array<FingerTableEntry>
    # successor_table: array<Node>
    # predecessor: Node

    # data_table : dict<string>:string

    def __init__(self, host, port):
        self.id = generate_hash(host, port)
        self.host = host
        self.port = port
        self.server = None
        self.serve()

    # TODO : join 함수 구현 (우선순위 높음)
    def join_cluster(self, join_address):
        print(f'join from {self.host}:{self.port} to {join_address}')
        with grpc.insecure_channel(join_address) as channel:
            stub = chord_pb2_grpc.NodeStub(channel)
            response = stub.SayHello(chord_pb2.HelloRequest(name='hello', age=15))
        print("Greeter client received: " + response.message)

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

        print(f'ChordServer [{self.host}] listening on {self.port}')
        # self.server.wait_for_termination()

        # input thread
        threading.Thread(target=self.listen_command()).start()
