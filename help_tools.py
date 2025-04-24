import time
import pandas as pd
import requests
from datetime import date
import ast
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

######### TMDB
def get_movie_info(api_key, movie_id, get_director = True):

    url = f'https://api.themoviedb.org/3/movie/{movie_id}'
    params = {'api_key': api_key}
    data = requests.get(url, params=params).json()

    # ключевые параметры фильма
    genres = str([s['name'] for s in data.get('genres', [])])
    countries = [s['name'] for s in data.get('production_countries', [])]
    if len(countries) == 1:
        countries = countries[0]
    countries = str(countries)
    runtime = data.get('runtime')
    movie_status = data.get('status')
    imdb_id = data.get('imdb_id')

    if get_director == True:
        # достаем режиссера фильма
        url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits'
        params = {'api_key': api_key}
        data = requests.get(url, params=params).json()
        directors = [person['name'] for person in data['crew'] if person['job'] == 'Director']
        if len(directors) == 1:
            directors = directors[0]
        directors = str(directors)
        return [movie_id, movie_status, runtime, genres, countries, directors, imdb_id]
    else:
        return [movie_id, movie_status, runtime, genres, countries, imdb_id]


def get_one_actor_filmo(api_key, actor_id, excl_films = [], flt_dict = {}):

    credits_url = f'https://api.themoviedb.org/3/person/{actor_id}/movie_credits'
    movies = requests.get(credits_url, params={'api_key': api_key, 'language' : flt_dict.get('language')}).json()['cast']

    # парсим фильмографию
    info = {}
    for field in ['title', 'original_title', 'release_date', 'id', 'vote_average', 'vote_count']:
        info[field] = [s.get(field) for s in movies]
    df_stat = pd.DataFrame(info).rename(columns={'id' : 'movie_id'})

    # предв фильтрация чтобы не тратить запросы
    df_stat = df_stat[
                        (df_stat.release_date > flt_dict.get('release_date_min', '1900-01-01'))
                        & (df_stat.release_date < date.today().strftime('%Y-%m-%d'))
                        & (df_stat.vote_count > flt_dict.get('vote_count_min', 0))
                        & (df_stat.vote_average > flt_dict.get('vote_average_min', 0))
                        & (~df_stat.movie_id.isin(excl_films))
                     ]

    # теперь подробнее по фильмам
    df_movie = pd.DataFrame(None, columns=['movie_id', 'movie_status', 'runtime', 'genres', 'countries', 'directors', 'imdb_id']); j=0
    for movie_id in df_stat.movie_id.values:
        try:
            result = get_movie_info(api_key, movie_id)
            df_movie.loc[j, :] = result
            j+=1
        except:
            pass

    return df_stat.merge(df_movie, on='movie_id', how='left')


def get_one_director_filmo(api_key, director_id, excl_films = [], flt_dict = {}):

    url = f'https://api.themoviedb.org/3/person/{director_id}/movie_credits'
    params = {'api_key': api_key, 'language' : flt_dict.get('language')}
    data = requests.get(url, params=params).json()
    data = [m for m in data['crew'] if m['job'] == 'Director']

    # парсим фильмографию
    info = {}
    for field in ['title', 'original_title', 'release_date', 'id', 'vote_average', 'vote_count']:
        info[field] = [s.get(field) for s in data]
    df_stat = pd.DataFrame(info).rename(columns={'id' : 'movie_id'})

    # предв фильтрация чтобы не тратить запросы
    df_stat = df_stat[
                        (df_stat.release_date > flt_dict.get('release_date_min', '1900-01-01'))
                        & (df_stat.release_date < date.today().strftime('%Y-%m-%d'))
                        & (df_stat.vote_count > flt_dict.get('vote_count_min', 0))
                        & (df_stat.vote_average > flt_dict.get('vote_average_min', 0))
                        & (~df_stat.movie_id.isin(excl_films))
                     ]

    # теперь подробнее по фильмам
    df_movie = pd.DataFrame(None, columns=['movie_id', 'movie_status', 'runtime', 'genres', 'countries', 'imdb_id']); j=0
    for movie_id in df_stat.movie_id.values:
        try:
            result = get_movie_info(api_key, movie_id, get_director = False)
            df_movie.loc[j, :] = result
            j+=1
        except:
            pass

    return df_stat.merge(df_movie, on='movie_id', how='left')



############
def get_actors_filmo(api_key, actor_dict, excl_films = [], flt_dict = {}):
    df_stat = pd.DataFrame(None, columns=['title', 'original_title', 'release_date', 'movie_id',
                                          'vote_average', 'vote_count',
                                           'movie_status', 'runtime', 'genres', 'countries', 'directors', 'imdb_id', 'person', 'person_type'])
    for actor_name in actor_dict.keys():
        tmp = get_one_actor_filmo(api_key, actor_dict[actor_name], excl_films = excl_films, flt_dict = flt_dict)
        tmp['person'] = actor_name
        tmp['person_type'] = 'actor'
        df_stat = pd.concat([df_stat, tmp], ignore_index=True)

    return df_stat

def get_directors_filmo(api_key, director_dict, excl_films = [], flt_dict = {}):
    df_stat = pd.DataFrame(None, columns=['title', 'original_title', 'release_date', 'movie_id',
                                          'vote_average', 'vote_count',
                                           'movie_status', 'runtime', 'genres', 'countries', 'imdb_id', 'person', 'person_type'])
    for director_name in director_dict.keys():
        tmp = get_one_director_filmo(api_key, director_dict[director_name], excl_films = excl_films, flt_dict = flt_dict)
        tmp['person'] = director_name
        tmp['person_type'] = 'director'
        df_stat = pd.concat([df_stat, tmp], ignore_index=True)
    return df_stat

def get_imdb_info(api_key_omdb, df):
    tmp = df.copy()
    awards_info = []
    imdb_rating = []
    for imdb_id in df.imdb_id.values:
        try:
            url = f'http://www.omdbapi.com/?i={imdb_id}&apikey={api_key_omdb}'
            data = requests.get(url).json()
            imdb_rating.append(data.get('imdbRating'))
            awards_info.append(data.get('Awards'))
        except:
            awards_info.append(None)
            imdb_rate.append(None)
    tmp['awards'] = awards_info
    tmp['imdb_rating'] = imdb_rating
    return tmp



########## пост обработка
def get_filter(df, **args):
    tmp = df.copy()
    for g in args['genres_flt']:
        tmp = tmp[~tmp.genres.apply(lambda x: g in x)]

    def get_imdbr(x):
        try:
            return float(x)
        except:
            return 10

    tmp = tmp[
            (~tmp.movie_id.isin(args['excl_films']))
              & (tmp.release_date > args['release_date_min'])
              & (tmp.vote_count > args['vote_count_min'])
              & (tmp.vote_average > args['vote_average_min'])
              & (tmp.runtime > args['runtime_min'])
              & (tmp.imdb_rating.apply(get_imdbr) > args['imdb_rating_min'])
             ]

    # reorder cols
    tmp = tmp[['person', 'person_type', 'title', 'original_title', 'directors', 'release_date', 'imdb_rating',
               'awards', 'genres', 'runtime', 'imdb_id', 'countries', 'movie_status', 'vote_average', 'vote_count', 'movie_id']]
    return tmp.sort_values(by=['person', 'release_date'], ascending=False).fillna('').drop(columns=args['drop_columns']).reset_index(drop=True)


########## SPOTIFY
def get_top_n_id_by_name(client_id, client_secret, artist_name = '', top_cnt = 2):
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    result = sp.search(q=f'artist:{artist_name}', type='artist', limit = top_cnt)
    return [f"""{s['name']} - {s['id']} - popularity: {s['popularity']}""" for s in result['artists']['items']]


def sp_get_updates(sp, artist_id):

    albums = sp.artist_albums(artist_id, album_type='album', limit=50)
    albums_list = albums['items']
    artist_name = albums['items'][0]['artists'][0]['name']
    total_albums_cnt = albums['total']

    offset = 50
    while offset < total_albums_cnt:
        albums_list_ = sp.artist_albums(artist_id, album_type='album', limit=50, offset = offset)['items']
        albums_list = albums_list + albums_list_
        offset += 50


    i = 0
    df_stat = pd.DataFrame(None, columns=['artist', 'album', 'release_date',
                                          'tracks_cnt', 'uri', 'album_artists', 'album_type'])
    for s in albums_list:

        if s['album_type'] not in ('compilation'):
            df_stat.loc[i, :] = artist_name, s['name'], \
                                s['release_date'], s['total_tracks'], \
                                s['uri'], str([j['name'] for j in s['artists']]), s['album_type']
            i+=1
    return df_stat

def sp_get_albums_info(client_id, client_secret, artist_list):
    # get secret on https://developer.spotify.com/dashboard/e7bd2deea29b4fe1a33392499c720f45
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
    df_stat = pd.DataFrame(None, columns=['artist', 'album', 'release_date', 'tracks_cnt', 'uri', 'album_artists', 'album_type'])
    for artist_id in artist_list:
        try:
            tmp = sp_get_updates(sp, artist_id)
        except:
            print(f'EXCEPTION = {artist_id}')
        time.sleep(0.1)
        df_stat = pd.concat([df_stat, tmp], ignore_index=True)
    return df_stat

def sp_get_filter(df_stat,
                release_date_min = '2024-01-01',
                release_date_max = '2030-01-01',
                album_flt = ['deluxe', 'live', 'edition', 'soundtrack'],
                album_type_flt = ['single'],
                max_artists_in_album = 1,
                other_album_author = False,
                columns_excl = []
                 ):

    df_stat = df_stat[(df_stat.release_date > release_date_min)
                      & (df_stat.release_date < release_date_max)
                      & (df_stat.album.apply(lambda x: all(sub.lower() not in x.lower() for sub in album_flt)))
                      & (df_stat.album_artists.apply(lambda x: len(ast.literal_eval(x)) <= max_artists_in_album))
                      & (~df_stat.album_type.isin(album_type_flt))
                    ]

    if other_album_author == False:
        df_stat = df_stat[df_stat.apply(lambda x: x['artist'] in ast.literal_eval(x['album_artists']), axis=1)]

    return df_stat.drop_duplicates().sort_values(by=['artist', 'release_date'], ascending=False).drop(columns=columns_excl).reset_index(drop=True)
