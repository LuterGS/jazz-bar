import grpc
import threading
from concurrent import futures
from service import NodeService

from protos.output import chord_pb2_grpc

class Node:
    # id : int
    # host : string
    # port : string
    pass

class FingerTableEntry:
    # start : int
    # sucessor : Node
    pass

class ChordNode(Node):
    # id : int
    # host : string
    # port : string
    # server : grpc.server

    # finger_table : array<FingerTableEntry>
    # successor_table: array<Node>
    # predecessor: Node

    # data_table : Anything

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server = None
        self.serve()

    def listen_command(self):
        try:
            while True:
                command = input(">")
                print("[ECHO]"+ command)
                # time.sleep(1000)
        except KeyboardInterrupt:
            print('Bye')
            self.server.stop(0)

    def serve(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        chord_pb2_grpc.add_NodeServicer_to_server(NodeService(), self.server)
        address = self.host+":"+self.port
        self.server.add_insecure_port(address)
        self.server.start()
        print(f'ChordServer [{self.host}] listening on {self.port}')
        # self.server.wait_for_termination()
        threading.Thread(target=self.listen_command()).start()
