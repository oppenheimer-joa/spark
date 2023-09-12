# spark

해당 BR은 릴리즈 버전이며, 
구현이 되어있는 기능은 아래와 같습니다.

- IMDB[academy, venice, busan, cannes] 데이터 전처리
- IMDB 전처리 데이터 JOIN (feat.Partition by festa.name)
- TMDB[movieSimilar, movieDetails, movieCredits, movieImages, peopleDetails] 전처리
- TMDB[movieSimilar, movieDetails, movieCredits, movieImages] 전처리 데이터 JOIN
- TMDB peopleDetails 전처리 데이터 JOIN (feat.Partition by date.gte("YYYY")


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
