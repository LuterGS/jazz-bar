# jazz-bar
2021 1학기 분산시스템 및 컴퓨팅 기말 팀 프로젝트

### How to run

**Compile Protocol Buffer**

- `Module Not Found Error` 발생 시 `*_pb2_grpc.py`에서 모듈 경로 수정

```bash
python -m grpc_tools.protoc -I./protos --python_out=./protos/output --grpc_python_out=./protos/output ./protos/chord.proto
```