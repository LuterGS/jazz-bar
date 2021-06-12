# jazz-bar
2021 1학기 분산시스템 및 컴퓨팅 기말 팀 프로젝트

## How to use
- `get`
    - ```shell script
      get key         # key에 해당하는 Value를 가져옴
      ```

- `set`
    - ```shell script
      set key value   # {key : value} 를 DHT 에 저장 
      ```
      - key가 존재하는 경우, value만 update 

- `delete`
    - ```shell script
      delete key      # key에 해당하는 {key : value} DHT에서 삭제
      ```

- `join`
    - ```shell script
      join host:port  # 기존의 DHT 테이블에 접근 요청      
      ```
      - host는 IP, port는 실행중인 port 번호

- `disjoin`
    - ```shell script
      disjoin         # 현재 DHT 에서 나감 (데이터 모두 삭제) 
      ```
- `show`
    - ```shell script
      show            # 현재 노드의 Finger Table을 모두 출력
      ```
- `summary`
    - ```shell script
      summary         # 현재 노드의 Data를 모두 출력
      ```
- `ft_update`
    - ```shell script
      ft_update       # 현재 노드의 Finger Table을 업데이트
      ```

## How to run

**Compile Protocol Buffer**

- `Module Not Found Error` 발생 시 `*_pb2_grpc.py`에서 모듈 경로 수정

```shell script
python -m grpc_tools.protoc -I./protos --python_out=./protos/output --grpc_python_out=./protos/output ./protos/chord.proto
```

**Run in command line**

```shell script
python main.py --host localhost --port 50051
python main.py --host localhost --port 50052
```