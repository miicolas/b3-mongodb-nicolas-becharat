
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

URI = os.getenv('MONGODB_URI', "mongodb+srv://miicolas:RZxa4PS1bQgUHAz7@cluster0.ljlpabs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

client = MongoClient(URI, server_api=ServerApi('1'))
db = client.get_database('sample_mflix')

def preview(result, title_prefix="", show_details=False):
    if title_prefix:
        print(title_prefix)
        print("-" * 50)

    count = 0
    for movie in result:
        count += 1
        title = movie.get('title', 'Titre inconnu')
        year = movie.get('year', 'Année inconnue')
        genres = ', '.join(movie.get('genres', [])) if movie.get('genres') else 'Genres inconnus'
        directors = ', '.join(movie.get('directors', [])) if movie.get('directors') else 'Réalisateur inconnu'

        print(f"{count}. {title} ({year})")

        if show_details:
            print(f"   Genres: {genres}")
            print(f"   Réalisateur(s): {directors}")
            print()

    return count

def list(collection_name, query_filter, projection=None, options=None):
    """Récupère plusieurs documents d'une collection"""
    try:
        collection = db[collection_name]
        cursor = collection.find(query_filter, projection)

        if options:
            if 'sort' in options:
                cursor = cursor.sort(options['sort'])
            if 'limit' in options:
                cursor = cursor.limit(options['limit'])

        return cursor
    except Exception as e:
        print(f"Erreur lors de la récupération des données: {e}")
        return []

def get(collection_name, query_filter, projection=None):
    """Récupère un seul document d'une collection"""
    try:
        collection = db[collection_name]
        return collection.find_one(query_filter, projection)
    except Exception as e:
        print(f"Erreur lors de la récupération du document: {e}")
        return None

def post(collection_name, data):
    """Insère un document dans une collection"""
    try:
        collection = db[collection_name]
        return collection.insert_one(data)
    except Exception as e:
        print(f"Erreur lors de l'insertion: {e}")
        return None

def get_mongo_operator(operator):
    """Convertit un opérateur en opérateur MongoDB"""
    operators_map = {
        '>': '$gt',
        '<': '$lt',
        '>=': '$gte',
        '<=': '$lte',
        '==': '$eq',
        '!=': '$ne'
    }
    return operators_map.get(operator, '$eq')


# Exercice 1
def list_movies_by_year(year):
    """Liste les films sortis une année donnée"""
    result = list('movies', {'year': year})
    count = preview(result, f"Films sortis en {year}:")
    print(f"Total: {count} films trouvés pour l'année {year}")
    return result

# Exercice 2
def list_movies_where_genre_is(genre):
    """Liste les films d'un genre donné"""
    result = list('movies', {'genres': genre})
    count = preview(result, f"Films de genre {genre}:", show_details=True)
    print(f"Total: {count} films trouvés pour le genre {genre}")
    return result

# Exercice 3
def get_movie_by_title(title):
    """Récupère un film par son titre"""
    result = get('movies', {'title': title})
    if result:
        print(f"Film trouvé: {result.get('title', 'Titre inconnu')}")
    else:
        print(f"Aucun film trouvé avec le titre: {title}")
    return result

# Exercice 4
def list_movies_by_runtime(runtime, op):
    operator_mongo = get_mongo_operator(op)
    result = list('movies', {'runtime': {operator_mongo: runtime}})
    count = preview(result, f"Films avec une durée de {runtime} minutes {op}:", show_details=True)
    print(f"Total: {count} films trouvés pour la durée {runtime} minutes {op}")
    return result

def list_movies_title_year():
    """Liste tous les films avec titre et année"""
    result = list('movies', {})
    count = preview(result, "Films:", show_details=False)
    print(f"Total: {count} films trouvés")
    return result

def list_movies_imdb_rating(imdb_rating, op):
    operator_mongo = get_mongo_operator(op)
    result = list('movies', {'imdb.rating': {operator_mongo: imdb_rating}})
    count = preview(result, f"Films avec une note IMDB de {imdb_rating} {op}:", show_details=True)
    print(f"Total: {count} films trouvés pour la note IMDB {imdb_rating} {op}")
    return result

def list_movies_between_date(date1, date2):
    result = list('movies', {'year': {'$gte': date1, '$lte': date2}})
    count = preview(result, f"Films entre {date1} et {date2}:", show_details=True)
    print(f"Total: {count} films trouvés entre {date1} et {date2}")
    return result
def list_movies_genres(genres):
    genres_list = [genre.strip() for genre in genres.split(',')]
    result = list('movies', {'genres': {'$all': genres_list}})
    count = preview(result, f"Films de genre {genres}:", show_details=True)
    print(f"Total: {count} films trouvés pour le genre {genres}")
    return result

# Exercice 9
def list_movies_with_actor(actor_name):
    result = list('movies', {'cast': actor_name})
    count = preview(result, f"Films avec {actor_name} dans le cast:", show_details=True)
    print(f"Total: {count} films trouvés avec {actor_name}")
    return result

# Exercice 10
def list_movies_with_plot_keyword(keyword):
    result = list('movies', {'plot': {'$regex': keyword, '$options': 'i'}})
    count = preview(result, f"Films avec '{keyword}' dans le résumé:", show_details=True)
    print(f"Total: {count} films trouvés avec '{keyword}' dans le résumé")
    return result

# Exercice 11
def list_top_rated_movies(limit=10):
    options = {'sort': [('imdb.rating', -1)], 'limit': limit}
    result = list('movies', {'imdb.rating': {'$exists': True}}, options=options)
    count = preview(result, f"Top {limit} films les mieux notés:", show_details=True)
    print(f"Total: {count} films affichés")
    return result

# Exercice 12
def list_most_recent_movies(limit=5):
    options = {'sort': [('year', -1)], 'limit': limit}
    result = list('movies', {'year': {'$exists': True}}, options=options)
    count = preview(result, f"Top {limit} films les plus récents:", show_details=True)
    print(f"Total: {count} films affichés")
    return result

# Exercice 13
def list_longest_comedies():
    options = {'sort': [('runtime', -1)], 'limit': 5}
    result = list('movies', {'genres': 'Comedy', 'runtime': {'$exists': True}}, options=options)
    count = preview(result, "Comédies avec le plus long runtime:", show_details=True)
    print(f"Total: {count} comédies affichées")
    return result

# Exercice 14
def count_movies_by_genre():
    """Compte le nombre de films par genre"""
    collection = db['movies']
    pipeline = [
        {'$unwind': '$genres'},
        {'$group': {'_id': '$genres', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ]
    result = collection.aggregate(pipeline)
    print("Nombre de films par genre:")
    print("-" * 40)
    for genre_count in result:
        print(f"{genre_count['_id']}: {genre_count['count']} films")
    return result

# Exercice 15
def average_rating_by_genre():
    """Calcule la note moyenne par genre"""
    collection = db['movies']
    pipeline = [
        {'$match': {'imdb.rating': {'$exists': True}}},
        {'$unwind': '$genres'},
        {'$group': {'_id': '$genres', 'avg_rating': {'$avg': '$imdb.rating'}}},
        {'$sort': {'avg_rating': -1}}
    ]
    result = collection.aggregate(pipeline)
    print("Note moyenne IMDb par genre:")
    print("-" * 40)
    for genre_avg in result:
        print(f"{genre_avg['_id']}: {genre_avg['avg_rating']:.2f}")
    return result

# Exercice 16
def most_frequent_actors(limit=10):
    """Trouve les acteurs les plus fréquents"""
    collection = db['movies']
    pipeline = [
        {'$unwind': '$cast'},
        {'$group': {'_id': '$cast', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': limit}
    ]
    result = collection.aggregate(pipeline)
    print(f"Top {limit} acteurs les plus fréquents:")
    print("-" * 40)
    for actor in result:
        print(f"{actor['_id']}: {actor['count']} films")
    return result

# Exercice 17
def count_comments_by_movie():
    """Compte les commentaires par film"""
    collection = db['comments']
    pipeline = [
        {'$group': {'_id': '$movie_id', 'comment_count': {'$sum': 1}}},
        {'$sort': {'comment_count': -1}},
        {'$limit': 10}
    ]
    result = collection.aggregate(pipeline)
    print("Top 10 films avec le plus de commentaires:")
    print("-" * 50)
    for movie in result:
        print(f"Film ID {movie['_id']}: {movie['comment_count']} commentaires")
    return result

# Exercice 18
def movie_with_most_votes():
    """Trouve le film avec le plus de votes IMDB"""
    collection = db['movies']
    result = collection.find({'imdb.votes': {'$exists': True}}).sort('imdb.votes', -1).limit(1)
    movie = next(result, None)
    if movie:
        print(f"Film avec le plus de votes IMDb:")
        print(f"Titre: {movie.get('title', 'Inconnu')}")
        print(f"Votes: {movie.get('imdb', {}).get('votes', 'Inconnu')}")
        print(f"Note: {movie.get('imdb', {}).get('rating', 'Inconnu')}")
    return movie

# Exercice 19
def movies_with_comments():
    """Trouve les films avec leurs commentaires"""
    movies_collection = db['movies']
    pipeline = [
        {
            '$lookup': {
                'from': 'comments',
                'localField': '_id',
                'foreignField': 'movie_id',
                'as': 'comments'
            }
        },
        {'$match': {'comments': {'$ne': []}}},
        {'$limit': 5}
    ]
    result = movies_collection.aggregate(pipeline)
    print("Films avec leurs commentaires:")
    print("-" * 50)
    for movie in result:
        title = movie.get('title', 'Titre inconnu')
        comment_count = len(movie.get('comments', []))
        print(f"Film: {title} - {comment_count} commentaires")
    return result

# Exercice 20
def movies_with_recent_comments():
    """Trouve les films avec des commentaires récents"""
    from datetime import datetime
    comments_collection = db['comments']

    pipeline = [
        {
            '$match': {
                'date': {'$gte': datetime(2015, 1, 1)}
            }
        },
        {
            '$group': {
                '_id': '$movie_id',
                'recent_comment_count': {'$sum': 1}
            }
        },
        {
            '$lookup': {
                'from': 'movies',
                'localField': '_id',
                'foreignField': '_id',
                'as': 'movie_info'
            }
        },
        {
            '$match': {
                'movie_info': {'$ne': []}
            }
        },
        {'$limit': 10}
    ]

    result = comments_collection.aggregate(pipeline)
    print("Films avec commentaires post-2015:")
    print("-" * 40)
    count = 0
    for item in result:
        count += 1
        movie = item['movie_info'][0] if item['movie_info'] else {}
        title = movie.get('title', 'Titre inconnu')
        comment_count = item['recent_comment_count']
        print(f"{count}. {title} - {comment_count} commentaires récents")
    return result

def user_favorite_movies():
    """Affiche les utilisateurs et leurs films les plus commentés"""
    comments_collection = db['comments']
    movies_collection = db['movies']

    pipeline = [
        {
            '$group': {
                '_id': {
                    'user_name': '$name',
                    'movie_id': '$movie_id'
                },
                'comment_count': {'$sum': 1}
            }
        },
        {
            '$sort': {'comment_count': -1}
        },
        {
            '$group': {
                '_id': '$_id.user_name',
                'most_commented_movies': {
                    '$push': {
                        'movie_id': '$_id.movie_id',
                        'comment_count': '$comment_count'
                    }
                },
                'total_comments': {'$sum': '$comment_count'}
            }
        },
        {
            '$addFields': {
                'top_movies': {'$slice': ['$most_commented_movies', 3]}
            }
        },
        {'$sort': {'total_comments': -1}},
        {'$limit': 10}
    ]

    try:
        user_movies = comments_collection.aggregate(pipeline)
        print("Utilisateurs et leurs films les plus commentés:")
        print("-" * 50)

        count = 0
        for user_data in user_movies:
            count += 1
            user_name = user_data['_id']
            total_comments = user_data['total_comments']

            print(f"{count}. {user_name} - {total_comments} commentaires au total")

            for i, movie_data in enumerate(user_data['top_movies']):
                movie_id = movie_data['movie_id']
                comment_count = movie_data['comment_count']

                movie = movies_collection.find_one({'_id': movie_id})
                if movie:
                    title = movie.get('title', 'Titre inconnu')
                    year = movie.get('year', 'N/A')
                    print(f"   {i+1}. {title} ({year}) - {comment_count} commentaires")
            print()

        if count == 0:
            print("Aucun utilisateur avec des commentaires trouvé.")

        return user_movies
    except Exception as e:
        print(f"Erreur lors de l'agrégation: {e}")
        return []

# Exercice 22
def comments_by_user():
    """Affiche les utilisateurs avec le plus de commentaires"""
    comments_collection = db['comments']

    pipeline = [
        {
            '$match': {
                '$or': [
                    {'name': {'$exists': True, '$ne': None, '$ne': ''}},
                    {'email': {'$exists': True, '$ne': None, '$ne': ''}}
                ]
            }
        },
        {
            '$group': {
                '_id': {
                    'name': '$name',
                    'email': '$email'
                },
                'comment_count': {'$sum': 1},
                'recent_comment': {'$max': '$date'}
            }
        },
        {
            '$addFields': {
                'user_identifier': {
                    '$ifNull': [
                        '$_id.name',
                        '$_id.email'
                    ]
                }
            }
        },
        {
            '$match': {
                'user_identifier': {'$ne': None}
            }
        },
        {'$sort': {'comment_count': -1}},
        {'$limit': 15}
    ]

    try:
        result = comments_collection.aggregate(pipeline)
        print("Top 15 utilisateurs avec le plus de commentaires:")
        print("-" * 60)
        count = 0
        for user in result:
            count += 1
            user_name = user.get('user_identifier', 'Utilisateur inconnu')
            comment_count = user['comment_count']
            recent_date = user.get('recent_comment', 'N/A')

            if recent_date and recent_date != 'N/A':
                try:
                    if hasattr(recent_date, 'strftime'):
                        recent_str = recent_date.strftime('%Y-%m-%d')
                    else:
                        recent_str = str(recent_date)
                except:
                    recent_str = 'N/A'
            else:
                recent_str = 'N/A'

            print(f"{count:2d}. {user_name[:30]:30} | {comment_count:3d} commentaires | Dernier: {recent_str}")

        if count == 0:
            print("Aucun utilisateur avec des commentaires trouvé.")

        return result
    except Exception as e:
        print(f"Erreur lors de l'agrégation: {e}")
        return []

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    
    
    # Exercice 1
    #list_movies_by_year(1999)

    # Exercice 2
    #list_movies_where_genre_is('Comedy')

    # Exercice 3
    #get_movie_by_title('The Matrix')

    # Exercice 4
    #list_movies_by_runtime(120, '>')

    # Exercice 5
    #list_movies_title_year()

    # Exercice 6 
    #list_movies_imdb_rating(8.0, '>')

    # Exercice 7
    #list_movies_between_date('1990', '2000')

    # Exercice 8
    #list_movies_genres('Action, Sci-Fi')

    # Exercice 9
    #list_movies_with_actor('Tom Hanks')

    # Exercice 10
    #list_movies_with_plot_keyword('space')

    # Exercice 11
    #list_top_rated_movies(10)

    # Exercice 12
    #list_most_recent_movies(5)

    # Exercice 13
    #list_longest_comedies()

    # Exercice 14
    #count_movies_by_genre()

    # Exercice 15
    #average_rating_by_genre()

    # Exercice 16
    #most_frequent_actors(10)

    # Exercice 17
    #count_comments_by_movie()

    # Exercice 18
    #movie_with_most_votes()

    # Exercice 19
    #movies_with_comments()

    # Exercice 20
    #movies_with_recent_comments()


    # Exercice 22
    #comments_by_user()



except Exception as e:
    print(e)





