import grpc
from grpc._channel import _InactiveRpcError
import threading
import logging
from concurrent import futures
from service import NodeService, HealthCheckService

from utils import generate_hash

from protos.output import chord_pb2
from protos.output import chord_pb2_grpc

import time




class Node:
    def __init__(self, id, host, port):
        self.id = id
        self.host = host
        self.port = port
    def get_address(self):
        return self.host + ":" + self.port

class TableEntry:
    """
	@@ -44,19 +50,101 @@ class ChordNode(Node):
    """
    def __init__(self, host, port):
        self.id = generate_hash(host, port)
        # print(self.id, type(self.id))
        super().__init__(self.id, host, port)
        self.server = None
        # self.predecessor = Node(self.id, self.host, self.port)
        # # self.successor = Node(self.id, self.host, self.port)
        # self.successor = Node(self.id, self.host, "50053")
        # self.double_successor = Node(self.id, self.host, self.port)
        # for testing
        if port == "50051":
            self.predecessor = Node(self.id, self.host, "50054")
            self.successor = Node(self.id, self.host, "50052")
            self.double_successor = Node(self.id, self.host, "50053")
        if port == "50052":
            self.predecessor = Node(self.id, self.host, "50051")
            self.successor = Node(self.id, self.host, "50053")
            self.double_successor = Node(self.id, self.host, "50054")
        if port == "50053":
            self.predecessor = Node(self.id, self.host, "50052")
            self.successor = Node(self.id, self.host, "50054")
            self.double_successor = Node(self.id, self.host, "50051")
        if port == "50054":
            self.predecessor = Node(self.id, self.host, "50053")
            self.successor = Node(self.id, self.host, "50051")
            self.double_successor = Node(self.id, self.host, "50052")
        self.node_table = [self.predecessor, self.successor, self.double_successor]
        self.for_log = ["predecessor", "sucessor", "double_successor"]

        self.serve()

    # TODO : join 함수 구현 (우선순위 높음)
    def join_cluster(self, join_address):

        logging.info(f'join from {self.host}:{self.port} to {join_address}')
        with grpc.insecure_channel(join_address) as channel:
            stub = chord_pb2_grpc.NodeStub(channel)
            response = stub.SayHello(chord_pb2.HelloRequest(name=join_address, age=15))
        logging.info(f'{self.host}:{self.port} received {response.message}')

    def node_health_check(self, node: Node):
        node_address = node.get_address()
        try:
            with grpc.insecure_channel(node_address) as channel:
                stub = chord_pb2_grpc.HealthCheckerStub(channel)
                response = stub.Check(chord_pb2.HealthCheck(ping=0))
            return True
        except _InactiveRpcError:
            return False

    def request_node_info(self, node: Node, which_info: int):
        # 0은 predecessor, 1는 successor, 2은 double_successor
        node_address = node.get_address()
        with grpc.insecure_channel(node_address) as channel:
            stub = chord_pb2_grpc.GetNodeValueStub(channel)
            response = stub.GetNodeVal(chord_pb2.NodeDetail(node_id=node.id, which_node=which_info))
        return response.node_id, response.node_host, response.node_port

    def notify_node_info(self, node: Node, which_node: int):
        node_address = node.get_address()
        with grpc.insecure_channel(node_address) as channel:
            stub = chord_pb2_grpc.NotifyNodeStub(channel)
            response = stub.NotifyNodeChanged(chord_pb2.NodeType(node_id=self.id, node_host=self.host, node_port=self.port, which_node=which_node))
        return response.pong

    def health_check(self):
        while True:
            print("predecessor : ", self.predecessor.host, self.predecessor.port)
            print("successor : ", self.successor.host, self.successor.port)
            print("double_successor : ", self.double_successor.host, self.double_successor.port)
            print()

            time.sleep(5)
            if self.node_health_check(self.predecessor):        # predecessor가 접속되지 않는다면
                # self.predecessor = None                         # 변경 요청이 올것이기 때문에, 기다리면 됨
                pass

            if not self.node_health_check(self.successor):      # successor가 접속되지 않는다면
                print("successor is out of connection")
                # double successor로 successor를 교체
                self.successor = self.double_successor
                print("successor is changed : ", self.successor.port)

                # successor에게 본인이 predecessor라고 알려줌
                self.notify_node_info(self.successor, 0)


                # double successor에게 successor를 물어본 후, 응답값을 저장함
                ids, host, port = self.request_node_info(self.successor, 1)
                self.double_successor = Node(ids, host, port)

            if not self.node_health_check(self.double_successor):       # 만약 double_successor가 접속되지 않는다면
                print("double successor is out of connection")
                ids, host, port = self.request_node_info(self.successor, 2)
                self.double_successor = Node(ids, host, port)


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
        chord_pb2_grpc.add_HealthCheckerServicer_to_server(HealthCheckService(), self.server)
        chord_pb2_grpc.add_GetNodeValueServicer_to_server(GRPCService(self), self.server)
        chord_pb2_grpc.add_NotifyNodeServicer_to_server(GRPCService(self), self.server)

        address = self.host+":"+self.port
        self.server.add_insecure_port(address)
        self.server.start()
        logging.info(f'ChordServer is listening on {self.host}:{self.port}')
        # self.server.wait_for_termination()

        # input thread
        threading.Thread(target=self.health_check()).start()
        # threading.Thread(target=self.listen_command()).start()


class GRPCService(chord_pb2_grpc.GetNodeValueServicer, chord_pb2_grpc.NotifyNodeServicer):
    def __init__(self, chord_node: ChordNode):
        self.chord_node = chord_node

    def GetNodeVal(self, request, context):
        which_node = int(request.which_node)
        id = self.chord_node.node_table[which_node].id
        host = self.chord_node.node_table[which_node].host
        port = self.chord_node.node_table[which_node].port
        return chord_pb2.NodeVal(node_id=id, node_host=host, node_port=port)

    def NotifyNodeChanged(self, request, context):
        which_node = int(request.which_node)
        self.chord_node.node_table[which_node].id = request.node_id
        self.chord_node.node_table[which_node].host = request.node_host
        self.chord_node.node_table[which_node].port = request.node_port
        logging.info(self.chord_node.for_log[which_node] + " has been modified.")
        return chord_pb2.HealthReply(pong=0)