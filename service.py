from concurrent import futures
import grpc
import logging

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

def node_health_check(node):
    node_address = node.get_address()
    try:
        with grpc.insecure_channel(node_address) as channel:
            stub = chord_pb2_grpc.HealthCheckerStub(channel)
            response = stub.Check(chord_pb2.HealthCheck(ping=0))
        return True
    except _InactiveRpcError:
        return False


def request_node_info(node, which_info: int):
    # 0은 predecessor, 1는 successor, 2은 double_successor
    node_address = node.get_address()
    with grpc.insecure_channel(node_address) as channel:
        stub = chord_pb2_grpc.GetNodeValueStub(channel)
        response = stub.GetNodeVal(chord_pb2.NodeDetail(node_id=node.id, which_node=which_info))
    return response.node_id, response.node_host, response.node_port


def notify_node_info(cur_node, node, which_node: int):
    node_address = node.get_address()
    with grpc.insecure_channel(node_address) as channel:
        stub = chord_pb2_grpc.NotifyNodeStub(channel)
        response = stub.NotifyNodeChanged(
            chord_pb2.NodeType(node_id=cur_node.id, node_host=cur_node.host, node_port=cur_node.port, which_node=which_node))
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