import grpc
import threading
import logging
import time
from concurrent import futures

from node_table import NodeTable
from service import HealthCheckService, GetNodeValueService, NotifyNodeService, TossMessageService, toss_message, \
    HandleDataService, data_request
from data_structure import Data, TableEntry

from utils import NodeType as n
from utils import TossMessageType as t
from utils import DataHandlingType as d
from utils import generate_hash

from protos.output import chord_pb2_grpc


class ChordNode:

    def __init__(self, address):
        self.server = None

        # 현재 이 서버가 구동되고 있는 port
        self.address = address

        # data table 생성
        self.data_table = TableEntry()

        # node table 생성
        self.node_table = NodeTable(generate_hash(self.address), self.address, self.data_table)

        # 기능 구현 대기
        self.command_listener = threading.Thread(target=self.listen_command)

        # 작동 시작
        self.serve()

    # TODO : Get/Set/Remove/Join에 대한 핸들링 추가 및 프로토콜 결정 (우선순위 높음)
    def command_handler(self, command):
        commands = command.split()

        if commands[0] == 'get':
            key = commands[1]
            try:
                value = self.data_table.get(key)
                logging.info(f"request key:{key[:10]}'s value is {value}, stored in {self.address}")
            except ValueError:
                # 먼저, 해당하는 finger table이 살아있는지, 죽었으면 그 밑에 노드로 계속 보내는 health check 작업이 필요하다.
                nearest_node = self.node_table.find_nearest_alive_node(key)
                data_request(self.node_table.cur_node, nearest_node, Data(key, ""), d.get)

        elif commands[0] == 'set':
            key, value = commands[1].split(":")
            key = generate_hash(key)
            # 만약 자기 자신에 넣어야 하면
            cur_key = self.node_table.cur_node.key
            successor_key = self.node_table.finger_table.entries[n.successor].key

            # 만약 자기 자신에 넣을 수 있으면 자기 자신에 넣음
            if cur_key <= key < successor_key or successor_key < cur_key <= key or key < successor_key < cur_key:
                self.data_table.set(key, value)
                logging.info(f"request key:{key[:10]}'s value is set to {value}, stored in {self.address}")
            # 아닐 경우 살아있는 가장 가까운 노드를 찾아서 넣음
            else:
                nearest_node = self.node_table.find_nearest_alive_node(key)
                data_request(self.node_table.cur_node, nearest_node, Data(key, value), d.set)

        elif commands[0] == 'delete':
            key = commands[1]
            try:
                self.data_table.delete(key)
                logging.info(f"request key:{key[:10]} is deleted from {self.address}")
            except ValueError:
                nearest_node = self.node_table.find_nearest_alive_node(key)
                data_request(self.node_table.cur_node, nearest_node, Data(key, ""), d.delete)

        elif commands[0] == 'join':
            toss_message(self.node_table.cur_node, Data("", commands[1]), t.join_node)
            logging.info(f"finishing join node, will update finger table...")
            toss_message(self.node_table.cur_node, self.node_table.finger_table.entries[n.successor], t.finger_table_setting, 1, t.join_node)

        elif commands[0] == 'disjoin':
            self.server.stop(0)  # HealthCheck를 못 받게 모든 서버 종료
            self.node_table.stop_flag = True    # health check 송신 종료
            logging.info('Waiting for other nodes to update their finger tables')
            time.sleep(10)  # 다른 Node들이 FingerTable을 업데이트할때까지 대기
            for entry in self.data_table.entries:
                threading.Thread(
                    target=data_request,
                    args=(self.node_table.cur_node, self.node_table.predecessor, entry, d.set)).start()
            toss_message(self.node_table.cur_node, self.node_table.finger_table.entries[n.successor], t.finger_table_setting, 1, t.left_node)

        elif commands[0] == 'show':  # 노드 테이블 정보 출력하는 기능 추가
            self.node_table.log_nodes()

        elif commands[0] == 'summary':
            self.data_table.summary()

        elif commands[0] == 'ft_update':
            toss_message(self.node_table.cur_node, self.node_table.finger_table.entries[0], t.finger_table_setting, 1)

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

        # 이 부분이 server에 메시지 핸들러들을 등록시킴
        chord_pb2_grpc.add_HealthCheckerServicer_to_server(HealthCheckService(self.node_table), self.server)
        chord_pb2_grpc.add_GetNodeValueServicer_to_server(GetNodeValueService(self.node_table), self.server)
        chord_pb2_grpc.add_NotifyNodeServicer_to_server(NotifyNodeService(self.node_table), self.server)
        chord_pb2_grpc.add_TossMessageServicer_to_server(TossMessageService(self.node_table), self.server)
        chord_pb2_grpc.add_HandleDataServicer_to_server(HandleDataService(self.node_table, self.data_table),
                                                        self.server)

        # 서버 포트 지정 및 서버 시작
        self.server.add_insecure_port(self.address)
        self.server.start()

        logging.info(f'ChordServer is listening on {self.address}')

        # 기능 시작 (thread 구분)
        self.command_listener.start()
        self.node_table.run()
