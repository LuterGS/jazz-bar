from typing import Tuple

import grpc
import threading
import logging
from data_structure import Data, TableEntry
from utils import NodeType as n
from utils import TossMessageType as t
from utils import DataHandlingType as d

from grpc._channel import _InactiveRpcError

from protos.output import chord_pb2
from protos.output import chord_pb2_grpc

"""
class Service:
    # server : grpc.server
    #
    def __init__(self):
        pass
    def serve(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
"""


def node_health_check(node: Data) -> bool:
    try:
        with grpc.insecure_channel(node.value) as channel:
            stub = chord_pb2_grpc.HealthCheckerStub(channel)
            response = stub.Check(chord_pb2.HealthCheck(ping=0))
        return True
    except _InactiveRpcError:
        return False


def request_node_info(node: Data, which_info: int) -> Tuple[str, str]:
    # which_info는 utils.NodeType 의 명세를 따름
    with grpc.insecure_channel(node.value) as channel:
        stub = chord_pb2_grpc.GetNodeValueStub(channel)
        response = stub.GetNodeVal(chord_pb2.NodeDetail(node_address=node.value, which_node=which_info))
    return response.node_key, response.node_address


def notify_node_info(target_node: Data, node_info: Data, which_node: int) -> int:
    # change_type 는 utils.NodeType 의 명세를 따름
    with grpc.insecure_channel(target_node.value) as channel:
        stub = chord_pb2_grpc.NotifyNodeStub(channel)
        response = stub.NotifyNodeChanged(chord_pb2.NodeType(
            node_key=node_info.key, node_address=node_info.value, which_node=which_node
        ))
    return response.pong


def toss_message(starter_node: Data, receive_node: Data, message_type: int) -> int:
    # message_type 는 utils.TossMessageType 의 명세를 따름
    print(starter_node.key, starter_node.value, message_type, receive_node.value)
    with grpc.insecure_channel(receive_node.value) as channel:
        stub = chord_pb2_grpc.TossMessageStub(channel)
        response = stub.TM(chord_pb2.Message(
            node_key=starter_node.key, node_address=starter_node.value, message_type=message_type
        ))
    return response.pong


def data_request(starter_node: Data, receive_node: Data, data: Data, data_handling_type: int) -> int:
    """
    chord.proto 의 HandleData 요청을 보내는 메소드
    :param starter_node: 처음 요청을 생성하는 노드, 함수를 호출할 시에는 호출한 노드를 넣는 것이 맞음
    :param receive_node: 요청을 받는 노드, 현재는 모두 chord_node 의 successor 를 할당했으나, 추후에 finger table 이 구현되면 달라질 수 있음
    :param data: 요청하는 데이터. get 이나 remove 시 value 는 비어도 됨
    :param data_handling_type: utils.DataHandlingType 의 명세를 따름 (단, get_result 를 호출해서 사용할 필요는 없으니 사용하지 않아도 됨
            -> 자세한건 186번째 HandleDataService 클래스 참고할 것
    :return: receive_node 가 값을 잘 처리했으면 0이 return 됨
    """
    # data_handling_type 는 utils.DataHandlingType 의 명세를 따름
    with grpc.insecure_channel(receive_node.value) as channel:
        stub = chord_pb2_grpc.HandleDataStub(channel)
        response = stub.GD(chord_pb2.StarterWithData(
            node_key=starter_node.key, node_address=starter_node.value,
            data_key=data.key, data_value=data.value,
            data_handling_type=data_handling_type
        ))
    return response.pong


class HealthCheckService(chord_pb2_grpc.HealthCheckerServicer):
    def __init__(self, node_table):
        self.node_table = node_table

    def Check(self, request, context):
        return chord_pb2.HealthReply(pong=0)


class GetNodeValueService(chord_pb2_grpc.GetNodeValueServicer):
    def __init__(self, node_table):
        self.node_table = node_table

    def GetNodeVal(self, request, context):
        if request.which_node == n.predecessor:
            return chord_pb2.NodeVal(
                node_key=self.node_table.predecessor.key, node_address=self.node_table.predecessor.value
            )
        else:
            node_data = self.node_table.finger_table.entries[request.which_node]
            key = node_data.key
            value = node_data.value
            return chord_pb2.NodeVal(node_key=key, node_address=value)


class NotifyNodeService(chord_pb2_grpc.NotifyNodeServicer):
    def __init__(self, node_table):
        self.node_table = node_table

    def NotifyNodeChanged(self, request, context):
        if request.which_node == n.predecessor:
            self.node_table.predecessor.update_info(request.node_key, request.node_address, -1)
        else:
            self.node_table.finger_table.entries[request.which_node].update_info(
                request.node_key, request.node_address, request.which_node
            )
        return chord_pb2.HealthReply(pong=0)


class TossMessageService(chord_pb2_grpc.TossMessageServicer):
    def __init__(self, node_table):
        self.node_table = node_table

    def notify_new_node_income(self, new_node: Data):
        # 본인이 새로운 노드를 추가해야 할 때 -> 본인의 successor 로 추가해야 할 때 처리방식

        # 1. 현재 본인의 기존 successor 에게, 새로운 노드가 predecessor 라고 알려줌
        notify_node_info(self.node_table.finger_table.entries[0], new_node, n.predecessor)

        # 2. 새 노드의 predecessor 를 본인으로,
        #    새 노드의 successor 를 본인의 기존 successor 로,
        #    새 노드의 double_successor 를 본인의 기존 double_successor 로 설정함
        #    추후에 finger table 을 사용할 때는, 반복문을 돌면서 그냥 finger table 자체를 할당해주면 됨.
        notify_node_info(new_node, self.node_table.cur_node, n.predecessor)
        notify_node_info(new_node, self.node_table.finger_table.entries[0], n.finger_table(0))
        notify_node_info(new_node, self.node_table.finger_table.entries[1], n.finger_table(1))

        # 3. 본인의 double successor 를 기존 successor 로, successor 를 새로운 노드로 업데이트
        self.node_table.finger_table.entries[1].update_info(
            self.node_table.finger_table.entries[0].key, self.node_table.finger_table.entries[0].value, n.finger_table(1)
        )
        self.node_table.finger_table.entries[0].update_info(new_node, 0)

        # 4. 본인의 predecessor 에게, double successor 가 새로운 노드임을 알려줌
        #    추후에 finger table을 사용할 때, 반복문을 돌면서 처리할 수는 있을거같음.
        threading.Thread(target=notify_node_info,
                         args=(
                             self.node_table.predecessor, new_node, n.finger_table(1),)
                         ).start()

    def TM(self, request, context):
        logging.info(f'Toss Message received from {request.node_address}')
        if request.message_type == t.join_node:
            # 만약 join일 시, finger table 에서의 insert 위치를 찾아본다.

            # 1. 본인의 key값보다 크고, successor (finger_table[0]) 의 key 값보다 작은 경우는, 내가 추가한다.
            if self.node_table.cur_node.key < request.node_key < self.node_table.finger_table.entries[0].key or \
                    self.node_table.finger_table.entries[0].key < self.node_table.cur_node.key < request.node_key:
                logging.info(f'Now Adding {request.node_address}...')
                self.notify_new_node_income(new_node=Data(request.node_key, request.node_address))
            # 2. 아닐 경우에는, 노드 테이블을 순회하면서 적절히 보낼 위치를 찾는다.
            # -> 일단 지금은, 바로 successor 에게 넘긴다. (finger table 의 속성을 변경하는 작업이 필요함)
            else:
                logging.info(f'Passing {request.node_address}`s message to {self.node_table.finger_table.entries[0].value}')
                threading.Thread(target=toss_message,
                                 args=(
                                     Data(request.node_key, request.node_address),
                                     self.node_table.finger_table.entries[0],
                                     request.message_type)
                                 ).start()
                # send_node = False
                # for i in range(len(self.finger_table.entries) - 1):
                #     if self.finger_table.entries[i].key < request.node_key < self.finger_table.entries[i + 1].key:
                #         send_node = self.finger_table.entries[i]
                #         break
                # if not send_node and self.finger_table.entries[-1].key < request.node_key:
                #     send_node = self.finger_table.entries[-1]
                #
                # threading.Thread(target=toss_message,
                #                  args=(Data(request.node_key, request.node_address), send_node,))
            return chord_pb2.HealthReply(pong=0)


class HandleDataService(chord_pb2_grpc.HandleDataServicer):
    def __init__(self, node_table, data_table: TableEntry):
        self.node_table = node_table
        self.data_table = data_table

    def get(self, starter_node: Data, req_data: Data):
        i = 0       # 의미없는 구문, 제거해도 됨
        """
        자신이 처리해야 할 get 요청을 처리하는 메소드이다.
        key 를 토대로 value 를 찾는다.
        
        try:
            value = self.data_table.get(req_data.key)
        except ValueError:
            value = ""          -> gRPC 통신 상에 오류가 날 수 있으며, 오류날 시에는 특정 문자로 교체한 뒤 아래 GD의 get_result 부분도 수정해줄 것
        finally:
            # 이후, 이 값을 starter node 에게 다시 전달해준다.
            -> 스레드로 data_request 함수를 실행하되, 이 때 starter node 는 본인의 노드, receive_node 가 starter node, data 는 찾은 값을 넣어준다.
        """
        pass



    def GD(self, request, context):
        job_type = request.data_handling_type
        starter_node = Data(request.node_key, request.node_address)
        data = Data(request.data_key, request.data_value)
        '''
        최우선적으로, 만약 job_type 가 get_result 다?
        그러면 get 에 대한 결과를 받은 것이므로, 결과를 출력해준다.
        -> 이 때, starter node 는 해당 data 를 가지고 있는 node 이다.
        if job_type == d.get_result:
            if data.value == "": data.value = "not found"
            logging.info(f'request key:{data.key[:10]}'s value is {data.value}, stored in {starter_node.key[:10]}:{starter_node.value}')
            return chord_pb2.HealthReply(pong=0)
        '''


        """
        일단, data 의 key 를 보고, 자신의 Data Table 에 접근하는 요청인지 파악한다.
        어떻게? -> 본인의 node key 보다 data key 가 크거나 같고, successor 의 node key 보다 작으면 자신이 처리해야 한다.
                !! 예외처리 : 본인의 successor 가 본인보다 key 값이 작을 때 (본인이 dht 에서 가장 큰 값일 때)
                    -> if문 안에 조건처리를 한 번 더 해야함.
                    
        만약 본인이 처리해야 하면, job_type 에 따라 get, 혹은 set 을 실행한다.
        if job_type == d.get:
            self.get(starter_node, data)        # -> 스레드 실행은 자유
        if job_type == d.set:
            self.data_table.set(data)           # -> 스레드 실행 X
        if job_type == d.remove:
            self.data_table.delete(data.key)    # -> 스레드 실행 X
            
        
                    
        만약, 본인이 가지고 있지 않으면, successor 에게 그대로 정보를 전달한다.
        단, 스레드 처리를 해서 넘긴다. 이 파일의 168번째에 선언되는 Thread 와 비슷하게 
        대충 쓰면, 이렇게 처리할 수 있다.
        threading.Thread(
            target=data_request, 
            args=(
                starter_node, 
                self.node_table.finger_table.entries[0], 
                data,
                job_type)
        ).start()
        """
        pass
