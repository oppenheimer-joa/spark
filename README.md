# spark

v0.3.0/yoda/dev BR은 수집 API 서버로 부터 S3에 적재된 rawFiles를 전처리하여, 
S3 bucket에 정제된 데이터를 적재하는 spark-job 이 기재되어있는 BR입니다.

## FileTree
구조는 아래와 같습니다.
```
.
├── README.md
├── config
│   └── __init__.py
├── lib
│   └── __init__.py
└── src
    └── __init__.py
```
