import grpc
from grpc._channel import _InactiveRpcError
import threading
import logging
from concurrent import futures

from node_table import NodeTable
from service import HealthCheckService, GetNodeValueService, NotifyNodeService, TossMessageService, node_health_check, notify_node_info, request_node_info, toss_message
from data_structure import Node

from utils import generate_hash

from protos.output import chord_pb2
from protos.output import chord_pb2_grpc

import time




class TableEntry:
    """
    자동정렬된 List를 가지고 있는 class
    구현해야하는 함수
    1. append : 리스트에 자동 정렬해서 값을 넣음
    2. find (or find_nearest) : 요청한 값에 가장 가까운 value를 찾아줌
    3. update (or modify) : Finger Table 하나의 값을 삭제 혹은 변경
    """
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
        ids = generate_hash(host, port)
        super().__init__(ids, host, port, "cur_node")
        self.server = None

        self.node_table = NodeTable(ids, host, port)
        self.serve()

    # TODO : join 함수 구현 (우선순위 높음)
    def join_cluster(self, join_address):
        join_address = join_address.split(":")
        new_node = Node("will replaced", join_address[0], join_address[1])
        toss_message(self, new_node, 1)


    # TODO : Get/Set/Remove/Join에 대한 핸들링 추가 및 프로토콜 결정 (우선순위 높음)
    def command_handler(self, command):
        commands = command.split()

        if commands[0] == 'get':
            print("GETGET")
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

        # 이 부분이 server
        chord_pb2_grpc.add_HealthCheckerServicer_to_server(HealthCheckService(self.node_table), self.server)
        chord_pb2_grpc.add_GetNodeValueServicer_to_server(GetNodeValueService(self.node_table), self.server)
        chord_pb2_grpc.add_NotifyNodeServicer_to_server(NotifyNodeService(self.node_table), self.server)
        chord_pb2_grpc.add_TossMessageServicer_to_server(TossMessageService(self.node_table), self.server)

        address = self.host+":"+self.port
        self.server.add_insecure_port(address)
        self.server.start()

        logging.info(f'ChordServer is listening on {self.host}:{self.port}')
        # self.server.wait_for_termination()

        # 기능 시작 (thread 구분)
        command_listener = threading.Thread(target=self.listen_command)
        health_check = threading.Thread(target=self.node_table.health_check)
        command_listener.start()
        health_check.start()


