import psycopg2
from connection import DATABASE, USER, PASSWORD, HOST, PORT

#masks the database connection information properly
connection = psycopg2.connect(database = DATABASE,
    user = USER,
    password = PASSWORD,
    host = HOST,
    port = PORT)

cursor = connection.cursor()