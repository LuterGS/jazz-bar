import grpc
import threading
import logging
from concurrent import futures

from node_table import NodeTable
from service import HealthCheckService, GetNodeValueService, NotifyNodeService, TossMessageService, toss_message
from data_structure import Data

from utils import TossMessageType as t
from utils import generate_hash

from protos.output import chord_pb2
from protos.output import chord_pb2_grpc


class ChordNode:
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

    def __init__(self, address):
        self.server = None
        self.address = address
        # node table을 만들었으며, node table 내에서 모든 연결이 일어남.
        self.node_table = NodeTable(generate_hash(self.address), self.address)
        self.serve()

    # TODO : join 함수 구현 (우선순위 높음)
    def join_cluster(self, join_address):
        new_node = Data("will replaced", join_address)
        toss_message(self.node_table.cur_node, new_node, t.join_node)

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
        elif commands[0] == 'show':         # 노드 테이블 정보 출력하는 기능 추가
            self.node_table.log_nodes()

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

        self.server.add_insecure_port(self.address)
        self.server.start()

        logging.info(f'ChordServer is listening on {self.address}')
        # self.server.wait_for_termination()

        # 기능 시작 (thread 구분)
        command_listener = threading.Thread(target=self.listen_command)
        health_check = threading.Thread(target=self.node_table.health_check)
        command_listener.start()
        health_check.start()
