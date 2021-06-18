# constants
import hashlib


def constant(func):
    def func_set(self, value):
        raise TypeError

    def func_get(self):
        return func(self)

    return property(func_get, func_set)


class _NodeType(object):
    @constant
    def predecessor(self):
        return -1

    def finger_table(self, i):
        return i

    @constant
    def successor(self):
        return 0

    @constant
    def d_successor(self):
        return 1


class _TossMessageType(object):
    @constant
    def join_node(self):
        """
        network에 최초로 join할 때 쓰는 규격입니다.
        """
        return 1

    @constant
    def finger_table_setting(self):
        """
        노드의 finger table을 업데이트 할 때 쓰는 규격입니다.
        """
        return 2

    @constant
    def left_node(self):
        """
        network에서 나갈 때 쓰는 규격입니다. (djsjoin)
        """
        return 3


class _DataHandlingType(object):
    @constant
    def get(self):
        """
        network에서 data를 가져올 때 사용합니다.
        """
        return 1

    @constant
    def set(self):
        """
        네트워크에서 data를 삽입/변경할 때 사용합니다.
        """
        return 2

    @constant
    def delete(self):
        """
        네트워크에서 data를 삭제할 때 사용합니다.
        """
        return 3

    @constant
    def get_result(self):
        """
        데이터를 가지고 있는 노드가 요청한 노드에게 데이터 값을 전송할 때 사용합니다.
        """
        return 4


NodeType = _NodeType()
TossMessageType = _TossMessageType()
DataHandlingType = _DataHandlingType()

HASH_BIT_LENGTH = 32


def generate_hash(address: str):
    hasher = hashlib.sha1()
    hasher.update(address.encode())
    return hasher.hexdigest()


# TODO: try-catch 시에 raw stack trace 출력 안 하고 error code랑 message만 출력 (우선순위 낮음)
class Error:
    def __init__(self, code, message):
        self.code = code
        self.message = message


def hanle_error():
    pass
