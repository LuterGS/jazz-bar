import logging
from threading import Lock
import bisect
from typing import List
from multipledispatch import dispatch

class Data:
    def __init__(self, key, value):
        self.lock = Lock()
        self.key = key
        self.value = value

    def __lt__(self, other):
        return self.key < other.key

    def __eq__(self, other):
        return self.key == other.key

    def __str__(self):
        return f'Key: {self.key}, Value: {self.value}'

    @dispatch(str, str, int)
    def update_info(self, ids, address, loc: int):
        logging.info(f'finger_table[{loc}] is updated, {self.key[:10]}:{self.value} to {ids[:10]}:{address}')
        with self.lock:
            self.__init__(ids, address)

    @dispatch(object, int)
    def update_info(self, data, loc: int):
        ids = data.key
        address = data.value
        logging.info(f'finger_table[{loc}] is updated, {self.key[:10]}:{self.value} to {ids[:10]}:{address}')
        with self.lock:
            self.__init__(ids, address)




class TableEntry:
    """
    자동정렬된 Data object list를 가지고 있는 class
    DataTable, FingerTable이 상속

    1. get: key를 가지는 Data 반환
    2. set: key를 가지는 Data의 value를 변경하거나, 존재하지 않는 경우 Data를 추가
    3. delete: key를 가지는 Data 삭제
    4. concat: Data object list를 기존의 list에 합침. (disjoin하는 Node의 데이터를 받아올때 필요할거 같음)
    """

    def __init__(self, entries: List[Data] = list()):
        self.entries = entries
        self.entries.sort()

    def summary(self):
        print(f'Number of Entries: {len(self.entries)}')
        for i, entry in enumerate(self.entries):
            print(i, str(entry))

    def index(self, key):
        i = bisect.bisect_left(self.entries, Data(key, 'dummy'))
        if i != len(self.entries) and self.entries[i].key == key:
            return i
        raise ValueError('key not found in table')

    def get(self, key):
        return self.entries[self.index(key)]

    def set(self, key, value):
        try:
            self.entries[self.index(key)] = Data(key, value)
        except ValueError:
            bisect.insort(self.entries, Data(key, value))

    # @dispatch(str, str, str)
    # def update_info(self, id, host, port):
    #     with self.lock:
    #         logging.info(f'{self.name} is updated, {self.id[:10]}:{self.get_address()} to {id[:10]}:{host}:{port}')
    #         self.__init__(id, host, port, self.name)
    #
    # @dispatch(object)
    # def update_info(self, node):
    #     ids = node.id
    #     host = node.host
    #     port = node.port
    #     with self.lock:
    #         logging.info(f'{self.name} is updated, {self.id[:10]}:{self.get_address()} to {id[:10]}:{host}:{port}')

    def delete(self, key):
        self.entries.pop(self.index(key))

    def concat(self, new_entries: List[Data], concat_type='sort'):
        if concat_type == 'trailing':
            self.entries = self.entries + new_entries
        elif concat_type == 'leading':
            self.entries = new_entries + self.entries
        elif concat_type == 'sort':
            self.entries += new_entries
            self.entries.sort()
        else:
            raise ValueError('concat_type = "trailing" or "leading" or "sort"')
