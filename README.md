# spark

## v0.9.0/rel

해당 BR은 릴리즈 버전이며, 
구현이 되어있는 기능은 아래와 같습니다.

### 전체 기능
v0.9.0
- IMDB[academy, venice, busan, cannes] 데이터 전처리
- IMDB 전처리 데이터 JOIN (feat.Partition by festa.name)
- TMDB[movieSimilar, movieDetails, movieCredits, movieImages, peopleDetails] 전처리
- TMDB[movieSimilar, movieDetails, movieCredits, movieImages] 전처리 데이터 JOIN
- TMDB peopleDetails 전처리 데이터 JOIN (feat.Partition by date.gte("YYYY")
- TMDB 영화별 장르 one-hot encoding
- KOPIS 데이터 전처리 (feat.Partition by genreCode)
- Spotify 데이터 전처리
- BoxOffice 데이터 전처리 (일별 집계)
- BoxOffice 전처리 데이터 월별 JOIN (feat.Partition by loc_code)


### FileTree
구조는 아래와 같습니다.
```
.
├── README.md
├── bak
│   ├── BoxOffice._test.py
│   ├── Imdb_academy.py
│   ├── Imdb_busan.py
│   ├── Imdb_cannes.py
│   ├── Imdb_transform_cannes.py
│   ├── Imdb_venice.py
│   ├── TMDB_split_movieCode.py
│   ├── Tmdb_split_rdd.py
│   ├── imdb_transform_academy_final.py
│   ├── imdb_transform_venice_final.py
│   ├── rdd_kopis_boto3.py
│   ├── test.py
│   ├── test_imdb.py
│   └── venice_pandas_test.py
├── config
│   └── config.ini
│   └── modules.py
├── sh
│   ├── boxoffice_pyspark.sh
│   ├── imdb_join_parquet_pyspark.sh
│   ├── imdb_transform_pyspark.sh
│   ├── join_boxOffice.sh
│   ├── kopis_pyspark.sh
│   ├── spotify_pyspark.sh
│   ├── tmdb_genre_pyspark.sh
│   ├── tmdb_join_pyspark.sh
│   ├── tmdb_people_join_pyspark.sh
│   ├── tmdb_people_pyspark.sh
│   ├── tmdb_pyspark.sh
└── src
    ├── BoxOffice_join_data.py
    ├── BoxOffice_transform_data.py
    ├── Imdb_join_parquet.py
    ├── Imdb_transform.py
    ├── Kopis_transform_data.py
    ├── Spotify_transform_data.py
    ├── Tmdb_extract_genres.py
    ├── Tmdb_join.py
    ├── Tmdb_join_people.py
    ├── Tmdb_transform_credit.py
    ├── Tmdb_transform_details.py
    ├── Tmdb_transform_images.py
    ├── Tmdb_transform_people.py
    └── Tmdb_transform_similar.py
```
