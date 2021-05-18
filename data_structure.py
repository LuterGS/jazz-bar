import logging
from threading import Lock
import bisect
from typing import List
from multipledispatch import dispatch

class Data:
    # 기존의 Node를 Data로 합침 (어차피 DHT에서 모든 값은 key-value로 저장되기 때문에, 이런 방식을 택함)
    lock = Lock()  # 여러 스레드에서 접근할 수 있기 때문에 mutex lock선언

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __lt__(self, other):
        return self.key < other.key

    def __eq__(self, other):
        return self.key == other.key

    def __str__(self):
        return f'Key: {self.key}, Value: {self.value}'

    @dispatch(str, str, int)        # -> 메소드 오버로딩
    def update_info(self, key, value, loc: int):
        logging.info(f'finger_table[{loc}] is updated, {self.key[:10]}:{self.value} to {key[:10]}:{value}')
        with self.lock:     # 값을 변경할 때, 동시 접근이 존재할수도 있으므로, mutex lock을 건 상태에서 진행
            self.__init__(key, value)

    @dispatch(object, int)
    def update_info(self, data, loc: int):
        key = data.key
        value = data.value
        logging.info(f'finger_table[{loc}] is updated, {self.key[:10]}:{self.value} to {key[:10]}:{value}')
        with self.lock:  # 값을 변경할 때, 동시 접근이 존재할수도 있으므로, mutex lock을 건 상태에서 진행
            self.__init__(key, value)

    # Warning : update_info 사용 시, update_info(ids=val, address=val, loc=3) <- 이런 식으로 사용하지 말 것!
    # multidispatch 사용법이 익숙치 않아 해당 기능 구현 안됨
    # 반드시 update_info(ids, address, loc) 혹은 update_info(node, loc) 이렇게 사용할 것!


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
        logging.info(f'showing table entry... ')
        print(f'Number of Entries: {len(self.entries)}')
        for i, entry in enumerate(self.entries):
            print(i, str(entry))
        print()

    def index(self, key):
        i = bisect.bisect_left(self.entries, Data(key, 'dummy'))
        if i != len(self.entries) and self.entries[i].key == key:
            return i
        raise ValueError('key not found in table')

    def get(self, key):
        return self.entries[self.index(key)]

    def set(self, key, value):
        try:
            location = self.index(key)
            self.entries[location].update_info(key, value, location)
            # 기존 값을 교체할 때, race_condition safe 하기 위해 이렇게 처리
        except ValueError:
            bisect.insort(self.entries, Data(key, value))

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
