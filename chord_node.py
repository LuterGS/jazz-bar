import grpc
import threading
import logging
from concurrent import futures

from node_table import NodeTable
from service import HealthCheckService, GetNodeValueService, NotifyNodeService, TossMessageService, toss_message, \
    HandleDataService, data_request
from data_structure import Data, TableEntry

from utils import TossMessageType as t
from utils import DataHandlingType as d
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

        # node table을 만들었으며, node table 내에서 모든 연결이 일어남. (node_table = finger table + myself)
        self.node_table = NodeTable(generate_hash(self.address), self.address)

        # data table 생성
        self.data_table = TableEntry()
        self.serve()

    # TODO : Get/Set/Remove/Join에 대한 핸들링 추가 및 프로토콜 결정 (우선순위 높음)
    def command_handler(self, command):
        commands = command.split()

        if commands[0] == 'get':
            # command 입력이
            # get key 순으로 들어온다고 가정
            # 먼저 본인에게 있는지 확인하고, 본인에게 있을 시 해당 값 출력
            # 없을 시에는 하단 메소드를 참고, request 요청
            # data_request(self.node_table.cur_node, self.node_table.finger_table.entries[0], Data(commands[1], ""), d.get)
            pass
        elif commands[0] == 'set':
            # command 입력이
            # set key:value 순으로 들어온다고 가정
            # 먼저 본인에게 있는지 확인하고, 본인에게 있을 시 해당 값 출력
            # 없을 시에는 하단 메소드를 참고, request 요청
            key_value = commands[1].split(":")
            # data_request(self.node_table.cur_node, self.node_table.finger_table.entries[0], Data(key_value[0], key_value[1]), d.set)
            pass
        elif commands[0] == 'remove':
            # command 입력이
            # remove key 순으로 들어온다고 가정
            # 먼저 본인에게 있는지 확인하고, 본인에게 있을 시 해당 값 출력
            # 없을 시에는 하단 메소드를 참고, request 요청
            # data_request(self.node_table.cur_node, self.node_table.finger_table.entries[0], Data(commands[1], ""), d.remove)
            pass
        elif commands[0] == 'join':
            toss_message(self.node_table.cur_node, Data("", commands[1]), t.join_node)
        elif commands[0] == 'disjoin':
            # predecessor 에게 모든 정보를 넘겨줄 수 있음 (set 을 여러번 사용하는 것이 현재로썬 베스트, 아닐 경우엔 별도 통신 규격을 만들어야 함)
            pass
        elif commands[0] == 'show':         # 노드 테이블 정보 출력하는 기능 추가
            self.node_table.log_nodes()
        elif commands[0] == 'summary':
            self.node_table.finger_table.summary()

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
        chord_pb2_grpc.add_HandleDataServicer_to_server(HandleDataService(self.node_table, self.data_table), self.server)

        self.server.add_insecure_port(self.address)
        self.server.start()

        logging.info(f'ChordServer is listening on {self.address}')
        # self.server.wait_for_termination()

        # 기능 시작 (thread 구분)
        command_listener = threading.Thread(target=self.listen_command)
        health_check = threading.Thread(target=self.node_table.health_check)
        command_listener.start()
        health_check.start()
