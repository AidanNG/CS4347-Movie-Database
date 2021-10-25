import mysql.connector
from mysql.connector import Error

username = 'root'
password = 'Thermafit_sql'
database = 'moviedb'


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

if __name__ == "__main__":
	connection = create_db_connection('localhost', username, password, database)
	movie_insert = """
	INSERT INTO movie VALUES
	({},{},{},{});
	""".format(1,"'2010-07-14'","'a_movie'",20)
	execute_query(connection, movie_insert)

	movie_read = """
	SELECT *
	FROM movie;
	"""
	results = read_query(connection, movie_read)
	print(results)