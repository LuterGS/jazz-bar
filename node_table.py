import logging
import time
from concurrent import futures
from collections import deque

import grpc

from data_structure import Node
from service import notify_node_info, request_node_info, node_health_check
from utils import generate_hash
from utils import NodeType as n


class NodeTable:

    def __init__(self, ids, host, port):
        # generate node table
        self.cur_node = Node(ids, host, port)

        self.node_table = []
        self.for_log = ["predecessor", "successor", "double_successor"]
        for i in range(3):
            self.node_table.append(Node(ids, host, port, self.for_log[i]))
        if port == "50051":
            self.node_table[0] = Node("0b03a4d8a7d8f8f4c7afae9aeda7d76b431f4cba", host, "50054", "predecessor")
            self.node_table[1] = Node("0b03a4d8a7d8f8f4c7afae9aeda7d76b431f4cba", host, "50054", "successor")
        if port == "50054":
            self.node_table[0] = Node("a09b0ce42948043810a1f2cc7e7079aec7582f29", host, "50051", "predecessor")
            self.node_table[1] = Node("a09b0ce42948043810a1f2cc7e7079aec7582f29", host, "50051", "successor")

        self.health_check_status = [True, True, True]
        self.change_node_table = [self.change_predecessor, self.change_successor, self.change_double_successor]

        # for iteration
        self.cur_index = 0
        self.len_table = len(self.node_table)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            self.cur_index += 1
            return self.node_table[self.cur_index - 1]
        except IndexError:
            self.cur_index = 0
            raise StopIteration

    def __getitem__(self, item):
        return self.node_table[item]

    def __len__(self):
        return self.len_table

    def log_nodes(self):
        for node in self.node_table:
            logging.info(f'current {node.name} : {node.id[:10]}:{node.get_address()}')
        print()


    def change_predecessor(self, active: bool):
        if not active:
            pass

    def change_successor(self, active: bool):
        if not active:
            logging.info(f'successor:{self.node_table[n.successor].get_address()} is out of connection, change to {self.node_table[n.double_successor].get_address()}')
            self.node_table[n.successor].update_info(
                self.node_table[n.double_successor].id,
                self.node_table[n.double_successor].host,
                self.node_table[n.double_successor].port
            )

            notify_node_info(self.cur_node, self.node_table[n.successor], n.predecessor)

            d_id, d_host, d_port = request_node_info(self.node_table[n.successor], n.successor)
            self.node_table[n.double_successor].update_info(d_id, d_host, d_port)

    def change_double_successor(self, active: bool):
        if not active:
            logging.info(f"double successor:{self.node_table[n.double_successor].get_address()} is out of connection")
            d_id, d_host, d_port = request_node_info(self.node_table[n.successor], n.double_successor)
            self.node_table[n.double_successor].update_info(d_id, d_host, d_port)


    def health_check(self):
        self.log_nodes()
        while True:

            time.sleep(3)

            for i in range(3):
                self.health_check_status[i] = node_health_check(self.node_table[i])

            for i in range(3):
                self.change_node_table[i](self.health_check_status[i])

            if sum(self.health_check_status) != 3:
                self.log_nodes()

