from node import ChordNode
import argparse


def init_parser():
    # parser initialization
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=str, default="50051")
    return parser

# TODO : logger 추가
def init_logger():
    pass


if __name__ == '__main__':
    parser = init_parser()
    args = parser.parse_args()
    print(args.host, args.port)
    node = ChordNode(args.host, args.port)