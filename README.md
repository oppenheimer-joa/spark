# spark

v0.2.0/sub BR은 수집 API 서버로 부터 S3에 적재된 **kopis rawFiles**를 전처리하여, S3 bucket에 정제된 데이터를 적재하는 spark-job 이 기재되어있는 BR입니다.

## 전처리 내용

- styurls(소개이미지), tksites(예매처 링크) 형식 변경
- partitioning 위한 장르 코드 추가
```
genrecode={'연극':'theater', '뮤지컬' : 'musical' ,
           '서양음악(클래식)' : 'classic', '한국음악(국악)': 'korean', '대중음악' : 'popular',
           '무용' : 'dance', '대중무용' : 'dance',
           '서커스/마술' : 'extra', '복합' : 'extra' }
```

## FileTree
구조는 아래와 같습니다.
```
.
├── README.md
├── config
│   └── __init__.py
├── lib
│   └── __init__.py
│   └── modules.py
└── src
    └── __init__.py
    └── local_kopis.py
    └── rdd_kopis_boto3.py
    └── rdd_kopis_final.py
    └── rdd_kopis_s3aConn.py
```
