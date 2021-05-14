import bisect
from typing import List

class Data:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __lt__(self, other):
        return self.key < other.key

    def __eq__(self, other):
        return self.key == other.key

    def __str__(self):
        return f'Key: {self.key}, Value: {self.value}'

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


# for testing

# if __name__ == '__main__':
#     d1 = Data('a', 'd1')
#     d2 = Data('c', 'd2')
#     d3 = Data('e', 'd3')
#     d4 = Data('g', 'd4')
#     d5 = Data('i', 'd5')
#     d6 = Data('k', 'd6')
#     d7 = Data('m', 'd7')
#
#     table = TableEntry([d1, d2, d3, d4, d5, d6, d7])
#     table.summary()
#     print(str(table.get('a')))
#     table.set('b', 'd8')
#     table.set('c', 'd9')
#     table.delete('k')
#     table.summary()
#
#     d10 = Data('f', 'd10')
#     d11 = Data('j', 'd11')
#     d12 = Data('l', 'd12')
#     table.concat([d10, d11, d12])
#     table.summary()
