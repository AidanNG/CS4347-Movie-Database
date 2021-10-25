import csv
import requests
import os
import pickle
import mysql.connector
from mysql.connector import Error
from random import sample, randint

# iMDb api key
api_key = "k_sozuve0b"
# Database Login
user = 'root'
pw = 'Thermafit_sql'

# Movie indices
starting_index =171
ending_index = 220
# Maximum queries
max_querys = 50

movie_id = 171
employee_id = 1381

filename = './top_movies.pkl'
payload = {}
headers = {}

query_count = 0
songList = []

director_awards = [
    "Acadamy Award",
    "Oscar Award",
    "BAFA Award",
    "DGA Award",
    "Saturn Award",
    "AACTA Award",
    "Empire Award",
    "Independant Spirit Award"
]


# SQL Stuff
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


# Generate a random subset of director_awards
def generateRandomAwards():
    global director_awards
    length = randint(0, len(director_awards))
    return set(sample(director_awards, length))


# Generate a random song title and artist
# Returns in the form song, artist
def generateRandomMusicInfo():
    # Check if song list is populated
    if len(songList) <= 0:
        # If not, populate it
        readSongList()
    # Get a random number
    x = randint(0, 199)
    # Return a random song and artist
    return songList[x]['song'], songList[x]['artist']


def readSongList():
    with open('songlist.csv', mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            songList.append(row)


# Check if an employee with id 'employee' exists in table 'tableName'
def subclassExists(employee, tableName):
    read = """
	SELECT Employee_ID
	FROM {}
	WHERE Employee_ID = {};
	""".format(tableName, employee)
    return len(read_query(connection, read)) > 0


# Add an employee to the database
def addEmployeeToDatabase(firstname, lastname, connection):
    global employee_id

    # Check if the person already exists
    employee_read = """
	SELECT Employee_ID, FirstName, LastName
	FROM employee
	WHERE FirstName = '{}' AND LastName = '{}';
	""".format(firstname, lastname)
    employee_results = read_query(connection, employee_read)

    # If they to not exist, create them
    # Every employee gets a random salary between $100k and $1M
    if employee_results is None or len(employee_results) == 0:
        employee_insert = """
		INSERT INTO employee VALUES
		({},{},'{}','{}');
		""".format(employee_id, randint(100000, 1000000), firstname, lastname)
        execute_query(connection, employee_insert)

        # Increment the employee id
        employee_id += 1
        # Return the id of the newly created employee
        return (employee_id - 1)
    else:
        # If the employee already exists, return its id
        return employee_results[0][0]


# Add a worked on relation between 'employee' and 'movie'
def addWorkedOnToDatabase(employee, movie, connection):
    worked_insert = """
	INSERT INTO WORKEDON VALUES
	({}, {});
	""".format(movie, employee)
    execute_query(connection, worked_insert)


# Add a new actor to the database
def addActorToDatabase(actor, connection, movie_id):
    # If the person has only one name, it will be reported for first and last name
    name = actor['name'].split()
    # Get the id of the employee
    current_employee = addEmployeeToDatabase(name[0], name[-1], connection)

    # If this person is not already an actor, make them one
    if not subclassExists(current_employee, "actor"):
        actor_insert = """
		INSERT INTO actor VALUES
		({});
		""".format(current_employee)
        execute_query(connection, actor_insert)

    # Add the new role
    role_insert = """
	INSERT INTO role VALUES
	({}, '{}');
	""".format(current_employee, actor['asCharacter'])
    execute_query(connection, role_insert)

    # Add the worked on relation
    addWorkedOnToDatabase(current_employee, movie_id, connection)


# Add a new producer to the database
def addProducerToDatabase(producer, connection, movie_id):
    # If the person has only one name, it will be reported for first and last name
    name = producer['name'].split()
    # Get this person's employee id
    current_employee = addEmployeeToDatabase(name[0], name[-1], connection)

    # If this person is not already a producer, make them one
    # Every producer has a random budget between $1M and $10M
    if not subclassExists(current_employee, "producer"):
        producer_insert = """
		INSERT INTO producer VALUES
		({},{});
		""".format(current_employee, randint(1000000, 10000000))
        execute_query(connection, producer_insert)

    # Add the worked on relation
    addWorkedOnToDatabase(current_employee, movie_id, connection)


# Add a new director to the databse
def addDirectorToDatabase(director, connection, movie_id):
    # If the person has only one name, it will be reported for first and last name
    name = director['name'].split()
    # Get this person's employee id
    current_employee = addEmployeeToDatabase(name[0], name[-1], connection)

    # If this person is not already a director, make them one
    if not subclassExists(current_employee, "director"):
        director_insert = """
		INSERT INTO director VALUES
		({});
		""".format(current_employee)
        execute_query(connection, director_insert)

        # Give the director a random set of awards
        for award in generateRandomAwards():
            award_insert = """
			INSERT INTO awards VALUES
			({}, '{}');
			""".format(current_employee, award)
            execute_query(connection, award_insert)

    # Add the worked on relation
    addWorkedOnToDatabase(current_employee, movie_id, connection)


# Add a new trailer to the database
def addTrailerToDatabase(url, connection, movie_id):
    # Randomly generate a title and artist name for the music
    song_title, artist = generateRandomMusicInfo()

    # Create a new trailer. The trailer has a random length
    # of between 2 and 5 minutes.
    trailer_insert = """
	INSERT INTO trailer VALUES
	('{}', {}, {}, '{}', '{}');
	""".format(url, movie_id, randint(120, 300), artist, song_title)
    execute_query(connection, trailer_insert)


# Add a new genre to the database
def addGenreToDatabase(genre, connection, movie_id):
    genre_insert = """
	INSERT INTO genres VALUES
	({}, '{}');
	""".format(movie_id, genre)
    execute_query(connection, genre_insert)


# Add a new movie to the database
def addMovieToDatabase(movie, connection):
    global query_count
    global max_querys
    global movie_id
    global employee_id

    minutes = 0
    if movie['runtimeMins'] is not None and isinstance(movie['runtimeMins'], str):
        minutes = int(movie['runtimeMins'])

    # Add the movie to the database
    movie_insert = """
	INSERT INTO movie VALUES
	({},'{}','{}','{}');
	""".format(movie_id, movie['releaseDate'], movie['title'], minutes)
    execute_query(connection, movie_insert)

    # Create actor entries for the stars
    stars = []
    for star in movie['starList']:
        stars.append(star['id'])
    for actor in movie['actorList']:
        if actor['id'] in stars:
            addActorToDatabase(actor, connection, movie_id)

    # Create producer entries for the producers
    cast = movie['fullCast']['others']
    producers = []
    for member in cast:
        if member['job'] == 'Produced by':
            producers = member['items']
            break
    for producer in producers:
        addProducerToDatabase(producer, connection, movie_id)

    # Create director entries for the directors
    for director in movie['directorList']:
        addDirectorToDatabase(director, connection, movie_id)

    # Create genere entries for the genres
    for genre in movie['genres'].split(', '):
        addGenreToDatabase(genre, connection, movie_id)

    # If a trailer exists, add it
    try:
        addTrailerToDatabase(movie['trailer']['link'], connection, movie_id)
    except Exception:
        pass

    movie_id += 1


if __name__ == "__main__":
    top_movies = []
    # Check if the top movies cache file is available and load it
    # if not, create it
    if os.path.isfile(filename):
        print("Pulling pickled file")
        infile = open(filename, 'rb')
        top_movies = pickle.load(infile)
        infile.close()
        print(top_movies)
    else:
        print("Creating pickled file for top movies")
        top_movies_url = "https://imdb-api.com/en/API/Top250Movies/" + api_key
        top_movies = requests.request("GET", top_movies_url, headers=headers, data=payload).json()['items']
        query_count += 1
        outfile = open(filename, 'wb')
        pickle.dump(top_movies, outfile)
        outfile.close()

    # Establish connection to database
    connection = create_db_connection('localhost', user, pw, 'moviedb')

    # Query movies
    for index in range(starting_index, ending_index + 1):
        if query_count >= max_querys:
            print(f"Ran out of queries...")
            break
        movie_url = "https://imdb-api.com/en/API/Title/" + api_key + "/" + top_movies[index][
            'id'] + "/FullActor,FullCast,Trailer,"
        movie = requests.request("GET", movie_url, headers=headers, data=payload).json()
        query_count += 1
        addMovieToDatabase(movie, connection)

    # Print out the information for the next run
    print(f"The next index is '{index}'")
    print(f"The next movie_id is '{movie_id}'")
    print(f"The next employee_id is '{employee_id}'")