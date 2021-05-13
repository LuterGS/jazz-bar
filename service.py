from concurrent import futures
import grpc
import logging

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


class NodeService(chord_pb2_grpc.NodeServicer):
    def SayHello(self, request, context):
        logging.info(f'Received name → {request.name} [age] →{request.age}')
        return chord_pb2.HelloReply(message=f'Hello, {request.name} are {request.age} years old')


class HealthCheckService(chord_pb2_grpc.HealthCheckerServicer):
    def Check(self, request, context):
        # logging.info('received ping : ' + str(request.ping))
        return chord_pb2.HealthReply(pong=0)