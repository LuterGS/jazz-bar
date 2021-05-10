from concurrent import futures
import grpc

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
    async def SayHello(self, request, context):
        print("Received name: {0} age: {1}".format(request.name, request.age))
        return chord_pb2_grpc.HelloReply(message='Hello, {0} are {1} years old'.format(request.name, request.age))