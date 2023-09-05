from lib.modules import *
import json

#불러 와야할 file_key
# TMDB/credit/{year}/TMDB_movieDetails_{movieCode}_{year}.json
# 파일 명세
# TMDB_movieDetails_1000336_1960-01-01.json
# TMDB_movieCredits_1000336_1960-01-01.json
# TMDB_movieImages_1000336_1960-01-01.json
# TMDB_movieSimilar_1000336_1960-01-01.json
year = '1960-01-01'
movie_code = '1000336'
category = 'detail'

movie_similar_file = make_tmdb_file_dir(category, year, movie_code)
show_TMDB_data(movie_similar_file)
