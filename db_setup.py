"""
db_setup.py
language: python
author: Peter Jindra, peterfjindra@gmail.com

Sets up the database. This is meant to be run once only.
myMDb.py will ask to run this program when it fails to find an existing db
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from getpass import getpass

#ask for the user to input postgres password, so it isn't in the code
passw = getpass("Please enter the password for your postgres account:")

#first, we create the db to be used for the app
conn = psycopg2.connect(database="postgres", user="postgres", password=passw, host="127.0.0.1", port="5432")
print "Building database... This should take less than 60 seconds..."
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()
cur.execute('CREATE DATABASE test')
conn.commit()
conn.close()

#now, we log into our new db and build the tables
conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")

cur = conn.cursor()
cur.execute('''CREATE TABLE MOVIES
    (ID        SERIAL             PRIMARY KEY,
    TITLE      TEXT               NOT NULL   ,
    YEAR       TEXT                          ,
    RUNTIME    INT                           ,
    MPAA       TEXT                          ,
    RATING     REAL                          ,
    WATCHED    BOOLEAN                       ,
    OWN        BOOLEAN                  );''')
print "Movies table created successfully."

cur.execute('''CREATE TABLE ACTORS
	(ID        SERIAL             PRIMARY KEY,
	NAME       TEXT               NOT NULL);''')
print "Actors table created successfully."

cur.execute('''CREATE TABLE DIRECTORS
	(ID        SERIAL             PRIMARY KEY,
	NAME       TEXT               NOT NULL);''')
print "Directors table created successfully."

cur.execute('''CREATE TABLE WRITERS
	(ID        SERIAL             PRIMARY KEY,
	NAME       TEXT               NOT NULL);''')
print "Writers table created successfully."

cur.execute('''CREATE TABLE ACTING
	(ID        SERIAL             PRIMARY KEY,
	M_ID       INT                NOT NULL,
	A_ID       INT                NOT NULL);''')
print "M<->A association table created successfully."

cur.execute('''CREATE TABLE DIRECTING
	(ID        SERIAL             PRIMARY KEY,
	M_ID       INT                NOT NULL,
	D_ID       INT                NOT NULL);''')
print "D<->A association table created successfully."

cur.execute('''CREATE TABLE WRITING
	(ID        SERIAL             PRIMARY KEY,
	M_ID       INT                NOT NULL,
	W_ID       INT                NOT NULL);''')
print "W<->A association table created successfully."

conn.commit()
conn.close()