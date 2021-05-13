from concurrent import futures
import grpc
import threading
from threading import Lock
import logging
from data_structure import Node

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
    def __init__(self, chord_node):
        self.chord_node = chord_node

    def Check(self, request, context):
        return chord_pb2.HealthReply(pong=0)


class GetNodeValueService(chord_pb2_grpc.GetNodeValueServicer):
    def __init__(self, chord_node):
        self.chord_node = chord_node

    def GetNodeVal(self, request, context):
        which_node = int(request.which_node)
        id = self.chord_node.node_table[which_node].id
        host = self.chord_node.node_table[which_node].host
        port = self.chord_node.node_table[which_node].port
        return chord_pb2.NodeVal(node_id=id, node_host=host, node_port=port)


class NotifyNodeService(chord_pb2_grpc.NotifyNodeServicer):
    def __init__(self, chord_node):
        self.chord_node = chord_node

    def NotifyNodeChanged(self, request, context):
        which_node = int(request.which_node)
        self.chord_node.node_table[which_node].update_info(request.node_id, request.node_host, request.node_port)
        return chord_pb2.HealthReply(pong=0)


class TossMessageService(chord_pb2_grpc.TossMessageServicer):
    def __init__(self, chord_node):
        self.chord_node = chord_node

    def notify_new_node_income(self, new_node: Node):
        notify_node_info(new_node, self.chord_node.successor, 0)
        notify_node_info(self.chord_node, new_node, 0)
        notify_node_info(self.chord_node.successor, new_node, 1)
        notify_node_info(self.chord_node.double_successor, new_node, 2)
        self.chord_node.double_successor.update_info(self.chord_node.successor.id, self.chord_node.successor.host,
                                                     self.chord_node.successor.port)
        self.chord_node.successor.update_info(new_node.id, new_node.host, new_node.port)
        threading.Thread(target=notify_node_info,
                         args=(self.chord_node.successor, self.chord_node.predecessor, 2,)).start()

    def TM(self, request, context):
        logging.info(f'Toss Message received, {request.node_host}, {request.node_port}')
        if request.message_type == 1:
            new_node = Node(request.node_id, request.node_host, request.node_port)
            if self.chord_node.port < request.node_port < self.chord_node.successor.port or \
                    (self.chord_node.successor.port < self.chord_node.port < request.node_port):
                # 정상 경우일 때와
                # 끝 노드일 때도 확인해야함

                logging.info(f'adding {request.node_host}:{request.node_port}...')
                self.notify_new_node_income(new_node)
            else:
                threading.Thread(target=toss_message, args=(new_node, self.chord_node.successor, request.message_type,)).start()
            return chord_pb2.HealthReply(pong=0)