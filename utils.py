# constants
HASH_BIT_LENGTH = 32

# TODO: Host와 Port가 주어졌을 때 SHA1 기반의 해시 함수로 ID 생성 (우선순위 높음)
def generate_hash(host, port):
    address = f'{host}:{port}'
    return 2

# TODO: try-catch 시에 raw stack trace 출력 안 하고 error code랑 message만 출력 (우선순위 낮음)
class Error:
    def __init__(self, code, message):
        self.code = code
        self.message = message

def hanle_error():
    pass
