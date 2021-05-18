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


class _TossMessageType(object):
    @constant
    def join_node(self):
        return 1


class _DataHandlingType(object):
    @constant
    def get(self):
        return 1

    @constant
    def set(self):
        return 2

    @constant
    def remove(self):
        return 3

    @constant
    def get_result(self):
        return 4


NodeType = _NodeType()
TossMessageType = _TossMessageType()
DataHandlingType = _DataHandlingType()

HASH_BIT_LENGTH = 32
hasher = hashlib.sha1()


def generate_hash(address: str):
    hasher.update(address.encode())
    return hasher.hexdigest()


# TODO: try-catch 시에 raw stack trace 출력 안 하고 error code랑 message만 출력 (우선순위 낮음)
class Error:
    def __init__(self, code, message):
        self.code = code
        self.message = message


def hanle_error():
    pass
