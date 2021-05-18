import logging
import time
from copy import copy

from data_structure import TableEntry, Data
from service import request_node_info, node_health_check, notify_node_info
from utils import NodeType as n



class NodeTable:

    def __init__(self, ids, address: str):
        # generate node table
        host, port = address.split(":")

        # 현재 본인의 노드 정보와, predecessor 는 finger table에 없지만 필요하므로 별도로 선언함
        self.cur_node = Data(ids, address)


        # TODO : finger table은 key값에 상관없는 순서 보장이 필요함. 그래서 다른 방식의 저장이 필요하다고 생각됨.
        self.finger_table = TableEntry()

        # TODO : finger table은 중복이 되지 않지만, 초기 node init시에는 중복이 되어야 함.
        if port == "50051":
            self.predecessor = Data("0b03a4d8a7d8f8f4c7afae9aeda7d76b431f4cba", host + ":50054")
            self.finger_table.set("0b03a4d8a7d8f8f4c7afae9aeda7d76b431f4cba", host + ":50054")
            self.finger_table.entries.append(Data(ids, address))
            # self.finger_table.set("0b03a4d8a7d8f8f4c7afae9aeda7d76b431f4cba", host + ":50054") -> 사용 안됨!
        elif port == "50054":
            self.predecessor = Data("a09b0ce42948043810a1f2cc7e7079aec7582f29", host + ":50051")
            self.finger_table.set("a09b0ce42948043810a1f2cc7e7079aec7582f29", host + ":50051")
            self.finger_table.entries.append(Data(ids, address))
            # self.finger_table.set("a09b0ce42948043810a1f2cc7e7079aec7582f29", host + ":50051")
        else:
            self.predecessor = Data(ids, address)
            self.finger_table.set(ids, address)                      # 0 - successor
            self.finger_table.entries.append(Data(ids, address))     # 1 - double successor
            # self.finger_table.set(ids, address)   # -> 사용 안됨!
        self.health_check_status = [True for _ in range(len(self.finger_table.entries))]
        self.change_node_table = [self.change_successor, self.change_double_successor]

    def log_nodes(self):
        # predecessor를 별도로 관리하기 때문에, predecessor는 따로 로그를 찍어주고
        logging.info(f'current predecessor is {self.predecessor.key[:10]}:{self.predecessor.value}')
        i = 0
        # 이후에 finger table 값들을 출력함
        for node in self.finger_table.entries:
            logging.info(f'current finger_table[{i}] is {node.key[:10]}:{node.value}')
            i += 1
        print()

    def change_successor(self, active: bool):
        if not active:
            cur_successor = self.finger_table.entries[0]
            cur_double_successor = self.finger_table.entries[1]
            logging.info(f'successor {cur_successor.key[:10]}:{cur_successor.value} is out of connection, change to {cur_double_successor.key[:10]}:{cur_double_successor.value}')

            # successor 가 out of connection 이면,

            # 1. double_successor 에게 successor를 가져옴
            d_key, d_address = request_node_info(cur_double_successor, n.finger_table(0))

            # 2. 현재 노드의 successor를 double successor로 교체
            self.finger_table.entries[0].update_info(cur_double_successor.key, cur_double_successor.value, 0)

            # 3. 현재 노드의 double_successor를 double successor에게 받아온 정보로 교체
            self.finger_table.entries[1].update_info(d_key, d_address, 1)

            # 4. 새로 바뀐 successor에게, 본인이 predecessor라는 것을 알려줌
            notify_node_info(self.finger_table.entries[0], self.cur_node, n.predecessor)

    def change_double_successor(self, active: bool):
        if not active:
            cur_double_successor = self.finger_table.entries[1]
            logging.info(f"double successor {cur_double_successor.key[:10]}:{cur_double_successor.value} is out of connection")

            # double successor가 out of connection 이면,

            # 1. successor의 double_successor 정보를 받아옴
            d_key, d_address = request_node_info(self.finger_table.entries[0], n.finger_table(0))

            # 2. 받아온 정보를 현재 노드의 double successor에 저장
            self.finger_table.entries[1].update_info(d_key, d_address, 1)

    def health_check(self):
        self.log_nodes()
        while True:

            time.sleep(3)

            for i in range(2):
                self.health_check_status[i] = node_health_check(self.finger_table.entries[i])

            for i in range(2):
                self.change_node_table[i](self.health_check_status[i])

            if sum(self.health_check_status) != 2:
                self.log_nodes()
