# spark

v0.4.0/sub/dev BR은 수집 API 서버로 부터 S3에 적재된 **spotify rawFiles**를 전처리하여, S3 bucket에 정제된 데이터를 적재하는 spark-job 이 기재되어있는 BR입니다.

## 전처리 내용

- ['albums']['items'] 내 3개 아이템 추출하여 아래와 같은 데이터프레임 도출
```
movie_id  |  album1  | album2  |  album 3
```
- 각 앨범별 필요 columns 추출하여 {'name': name, 'artists': artists, 'url': url, 'image': image} 딕셔너리 형태로 반환
```
앨범명
아티스트명
spotify url
앨범 이미지 url
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
├── sh
│   └── spotify_pyspark.sh
└── src
    └── __init__.py
    └── Spotify_transform_data.py
