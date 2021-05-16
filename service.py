from concurrent import futures
import grpc
import threading
from threading import Lock
import logging
from data_structure import Node
from utils import NodeType as n
from utils import TossMessageType as t

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


def node_health_check(node: Node):
    node_address = node.get_address()
    try:
        with grpc.insecure_channel(node_address) as channel:
            stub = chord_pb2_grpc.HealthCheckerStub(channel)
            response = stub.Check(chord_pb2.HealthCheck(ping=0))
        return True
    except _InactiveRpcError:
        return False


def request_node_info(node: Node, which_info: int):
    # 0은 predecessor, 1는 successor, 2은 double_successor
    node_address = node.get_address()
    with grpc.insecure_channel(node_address) as channel:
        stub = chord_pb2_grpc.GetNodeValueStub(channel)
        response = stub.GetNodeVal(chord_pb2.NodeDetail(node_id=node.id, which_node=which_info))
    return response.node_id, response.node_host, response.node_port


def notify_node_info(change_node: Node, change_receive_node: Node, which_node: int):
    node_address = change_receive_node.get_address()
    with grpc.insecure_channel(node_address) as channel:
        stub = chord_pb2_grpc.NotifyNodeStub(channel)
        response = stub.NotifyNodeChanged(
            chord_pb2.NodeType(node_id=change_node.id, node_host=change_node.host, node_port=change_node.port,
                               which_node=which_node))
    return response.pong


def toss_message(starter_node: Node, receive_node: Node, message_type: int):
    node_address = receive_node.get_address()
    with grpc.insecure_channel(node_address) as channel:
        stub = chord_pb2_grpc.TossMessageStub(channel)
        response = stub.TM(
            chord_pb2.Message(node_id=starter_node.id, node_host=starter_node.host, node_port=starter_node.port,
                              message_type=message_type))
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
        which_node = request.which_node
        id = self.node_table[which_node].id
        host = self.node_table[which_node].host
        port = self.node_table[which_node].port
        return chord_pb2.NodeVal(node_id=id, node_host=host, node_port=port)


class NotifyNodeService(chord_pb2_grpc.NotifyNodeServicer):
    def __init__(self, node_table):
        self.node_table = node_table

    def NotifyNodeChanged(self, request, context):
        which_node = int(request.which_node)
        self.node_table[which_node].update_info(request.node_id, request.node_host, request.node_port)
        return chord_pb2.HealthReply(pong=0)


class TossMessageService(chord_pb2_grpc.TossMessageServicer):
    def __init__(self, node_table):
        self.node_table = node_table

    def notify_new_node_income(self, new_node: Node):
        notify_node_info(new_node, self.node_table[n.successor], n.predecessor)
        notify_node_info(self.node_table.cur_node, new_node, n.predecessor)
        notify_node_info(self.node_table[n.successor], new_node, n.successor)
        notify_node_info(self.node_table[n.double_successor], new_node, n.double_successor)
        self.node_table[n.double_successor].update_info(self.node_table[n.successor].id, self.node_table[n.successor].host,
                                                     self.node_table[n.successor].port)
        self.node_table[n.successor].update_info(new_node.id, new_node.host, new_node.port)
        threading.Thread(target=notify_node_info,
                         args=(self.node_table[n.successor], self.node_table[n.predecessor], n.double_successor,)).start()

    def TM(self, request, context):
        logging.info(f'Toss Message received, {request.node_host}, {request.node_port}')
        if request.message_type == t.join_node:
            new_node = Node(request.node_id, request.node_host, request.node_port)
            if self.node_table.cur_node.id < request.node_id < self.node_table[n.successor].id or \
                    (self.node_table[n.successor].id < self.node_table.cur_node.id < request.node_id):
                # 정상 경우일 때와
                # 끝 노드일 때도 확인해야함

                logging.info(f'adding {request.node_host}:{request.node_port}...')
                self.notify_new_node_income(new_node)
            else:
                threading.Thread(target=toss_message, args=(new_node, self.node_table[n.successor], request.message_type,)).start()
            return chord_pb2.HealthReply(pong=0)
