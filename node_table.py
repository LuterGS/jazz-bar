import logging
import math
import threading
import time

from data_structure import TableEntry, Data
from service import request_node_info, node_health_check, notify_node_info, toss_message, data_request
from utils import NodeType as n
from utils import TossMessageType as t
from utils import DataHandlingType as d


class NodeTable(threading.Thread):

    def __init__(self, ids, address: str, data_table: TableEntry):
        # generate node table
        super().__init__()
        host, port = address.split(":")

        # 현재 본인의 노드 정보와, predecessor 는 finger table에 없지만 필요하므로 별도로 선언함
        self.cur_node = Data(ids, address)

        # 현재 네트워크에 존재하는 노드 수를 정의
        self.node_network_num = 1  # 현재 존재하는 네트워크 노드 수

        # finger table 정의
        self.finger_table = TableEntry()

        # data table 정의
        self.data_table = data_table

        # stop flag 정의
        self.stop_flag = False

        # finger table update cycle 정의
        self.finger_table_update_cycle = 0

        # 보고서 상, 최초 init은 두 개의 노드가 있는 network이기 때문에, 해당 네트워크를 init해 줌
        if port == "50051":
            self.predecessor = Data("0b03a4d8a7d8f8f4c7afae9aeda7d76b431f4cba", host + ":50054")
            self.finger_table.append(self.predecessor)
            self.node_network_num += 1
        elif port == "50054":
            self.predecessor = Data("a09b0ce42948043810a1f2cc7e7079aec7582f29", host + ":50051")
            self.finger_table.append(self.predecessor)
            self.node_network_num += 1
        else:
            self.predecessor = Data(ids, address)
            self.finger_table.set(ids, address)  # 0 - successor
            # self.finger_table.entries.append(Data(ids, address))  # 1 - double successor

    def log_nodes(self):
        # TODO : TableEntry 클래스의 summary 를 이용해서 처리할 수 있지 않을까?
        # predecessor를 별도로 관리하기 때문에, predecessor는 따로 로그를 찍어주고
        logging.info(f'showing data table entry...')
        print(f'current predecessor is {self.predecessor.key[:10]}:{self.predecessor.value}')

        # 이후에 finger table 값들을 출력함
        for i, node in enumerate(self.finger_table.entries):
            print(f'current finger_table[{i}] is {node.key[:10]}:{node.value}')
        print()

    def find_nearest_alive_node(self, key):
        alive_node_num = 0
        alive_node_found = False

        # 가장 가까운 노드 값 찾기
        for i in range(len(self.finger_table.entries) - 1):
            if self.finger_table.entries[i].key > self.finger_table.entries[i + 1].key:
                if self.finger_table.entries[i].key <= key or key < self.finger_table.entries[i + 1].key:
                    alive_node_num = i
                    alive_node_found = True

            if self.finger_table.entries[i].key <= key < self.finger_table.entries[i + 1].key:
                alive_node_num = i
                alive_node_found = True

        if self.finger_table.entries[-1].key <= key and not alive_node_found:
            alive_node_num = len(self.finger_table.entries) - 1

        # health check를 통해, 죽어있으면 그것보다 앞의 노드를 return
        for i in range(alive_node_num, -1, -1):
            if node_health_check(self.finger_table.entries[i]):
                return self.finger_table.entries[i]

        # 만약 살아있는 노드가 없으면, 자기 자신을 return
        return self.cur_node

    def stabilize_successor(self):
        # 현재 health check를 통해, 제일 먼저 살아있는 successor 검출
        successor_num = 0
        successor_found = False
        for i in range(len(self.finger_table.entries)):
            if node_health_check(self.finger_table.entries[i]):
                successor_num = i
                successor_found = True
                break

        # 만약 successor가 살아있으면, health check를 그만둠
        if successor_num == 0 and successor_found:
            return True

        # 만약 successor가 죽어있으면, 교체하는 작업을 수행
        else:
            successor_node = self.finger_table.entries[successor_num]
            while True:
                # 1. 현재 최우선적으로 살아있는 노드에게 노드의 predecessor를 물어봄
                s_predecessor = request_node_info(successor_node, n.predecessor)
                if s_predecessor is False:
                    notify_node_info(successor_node, self.cur_node, n.predecessor)
                    self.finger_table.entries[n.successor].update_info(successor_node)
                    break

                print(f"successor : {successor_node.value}, s_predecessor : {s_predecessor.value}")

                # 만약에 그 predecessor가 응답하지 않으면, 그냥 자기 자신을 successor_node의 predecessor로 삼고 종료
                if node_health_check(s_predecessor) is False:
                    notify_node_info(successor_node, self.cur_node, n.predecessor)
                    print(successor_node.key, successor_node.value)
                    self.finger_table.entries[n.successor].update_info(successor_node.key, successor_node.value)
                    break

                # 2. 내 값과 predecessor의 값을 비교해, 같으면 내 finger table의 값만 수정해줌
                if s_predecessor.key == self.cur_node.key:
                    for i in range(successor_num):
                        self.finger_table.pop(i)
                        break

                # 3. 만약 내 값이 predecessor의 값보다 앞에 있으면, predecessor의 successor를 자신으로 설정하고, 자신의 successor를 predecessor의
                # successor로 설정
                elif self.cur_node.key > s_predecessor.key:
                    notify_node_info(s_predecessor, self.cur_node, n.successor)
                    notify_node_info(successor_node, self.cur_node, n.predecessor)
                    break

                # 4. 만약 내 값이 predecessor의 값보다 더 앞에 있으면, predecessor에게 predecessor를 물어보는 과정 반복
                else:
                    continue

    def finger_table_renew(self):
        # 네트워크 노드의 개수 차이를 구함
        # 현재 네트워크에 존재하는 노드의 개수에 따른 finger table node 개수와, 실제 finger table 개수가 맞지 않으면, 순회하는 코드를 사용
        if self.node_network_num == 1:
            return 0

        network_lens = math.log2(self.node_network_num)
        if int(network_lens) - network_lens != 0:
            network_lens = int(network_lens) + 1

        if network_lens - len(self.finger_table.entries) != 0:
            logging.info(f"will update finger table with ft_update...")
            toss_message(self.cur_node, self.finger_table.entries[n.successor], t.finger_table_setting, 1)
            self.finger_table_update_cycle = 0

        else:
            self.finger_table_update_cycle += 1
            return 0

        # 업데이트 이후 데이터 오류 맞춰줌
        pd_key = self.predecessor.key
        successor_key = self.finger_table.entries[n.successor].key
        cur_key = self.cur_node.key

        for i in range(len(self.data_table.entries)):
            data_key = self.data_table.entries[i].key
            if pd_key <= data_key < cur_key or cur_key < pd_key <= data_key or data_key < cur_key < pd_key:
                threading.Thread(target=data_request,
                                 args=(self.cur_node, self.predecessor, self.data_table.entries[i], d.set)
                                 ).start()
                self.data_table.delete(cur_key)

            if cur_key < successor_key <= data_key or successor_key <= data_key < cur_key:
                threading.Thread(target=data_request,
                                 args=(self.cur_node, self.finger_table.entries[n.successor], self.data_table.entries[i], d.set)
                                 ).start()
                self.data_table.delete(cur_key)

    def update_finger_table_info(self):
        self.log_nodes()
        while True:
            # network상에 메시지가 flooding을 막기 위해서 time 간격을 둠
            time.sleep(3)
            self.stabilize_successor()
            self.finger_table_renew()

            # 15초 간격으로 finger table 최신으로 업데이트 (주기 조정 가능)
            if self.finger_table_update_cycle > 5:
                toss_message(self.cur_node, self.finger_table.entries[n.successor], t.finger_table_setting, 1)
                self.finger_table_update_cycle = 0

            # 만약 disjoin이 일어나면 해당 cycle도 break
            if self.stop_flag:
                break

    def run(self):
        self.update_finger_table_info()
