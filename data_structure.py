import logging
from threading import Lock
# from multipledispatch import dispatch


class Node:
    def __init__(self, id, host, port, name="Node"):
        self.lock = Lock()
        self.id = id
        self.host = host
        self.port = port
        self.name = name
    def get_address(self):
        return self.host + ":" + self.port

    # @dispatch(str, str, str)
    def update_info(self, id, host, port):
        with self.lock:
            logging.info(f'{self.name} is updated, {self.get_address()} to {host}:{port}')
            self.__init__(id, host, port, self.name)