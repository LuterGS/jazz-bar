from chord_node import ChordNode
import logging
import argparse


def init_parser():
    # parser initialization
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=str, default="50051")
    return parser

# TODO : logger 추가
def init_logger():
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '[%(levelname)s] %(asctime)s (%(filename)s:%(lineno)d) : %(message)s'
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log.addHandler(handler)


if __name__ == '__main__':
    init_logger()
    parser = init_parser()
    args = parser.parse_args()
    node = ChordNode(args.host + ":" + args.port)