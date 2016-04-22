"""
db_personal.py
language: python
author: Peter Jindra, peterfjindra@gmail.com

A series of functions used for myMDb project.
These functions focus specifically on updating and querying the local PostgreSQL database.
"""
import psycopg2
from temp_objects import *

"""
Adds a person to ACTORS, DIRECTORS, or WRITERS
Note that an Actor, Director, and Writer are treated as separate persons, e.g.
Clint Eastwood might exist in the ACTORS and DIRECTORS tables,
and as far as the database knows, they are 2 separate entities.
@params:
	new_person: tempPerson object, the person to add to the db
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
@return:
	True if the person is added
	False if the person already existed
"""
def addPerson(new_person, passw):
	if hasPerson(new_person, passw):
		return False
	if new_person.p_type == "actor":
		table = "ACTORS"
	elif new_person.p_type == "director":
		table = "DIRECTORS"
	else:
		table = "WRITERS"
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("INSERT INTO " + table + "(NAME) VALUES  ('" + new_person.name + "')")
	conn.commit()
	conn.close()
	return True

"""
Adds a movie to MOVIES
Only title, year, runtime, mpaa, and rating are stored in the MOVIES table
Movies are connected to people with the ACTING, DIRECTING, and WRITING tables
@params:
	new_movie: tempMovie object, the movie to add to the db
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
@return:
	True if the movie was added
	False if the movie already existed
"""
def addMovie(new_movie, passw):
	if hasMovie(new_movie, passw):
		return False
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	values = "('" + new_movie.title + "','" + new_movie.year + "'," + str(new_movie.runtime) + ",'" + new_movie.mpaa + \
		    "'," + str(new_movie.rating) + "," + str(new_movie.watched) + "," + str(new_movie.own) + ")"
	#print "INSERT INTO MOVIES (TITLE,YEAR,RUNTIME,MPAA,RATING) VALUES " + values
	cur.execute("INSERT INTO MOVIES (TITLE,YEAR,RUNTIME,MPAA,RATING,WATCHED,OWN) VALUES " + values)
	conn.commit()
	conn.close()
	return True

"""
Adds foreign keys for a person and movie to ACTING, DIRECTING, or WRITING
@params:
	amovie: tempMovie object
	aperson: tempPerson object
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
@return:
	False if one of the two objects does not exist, entry is unsuccessful
	True if the entry is successful
"""
def addRole(amovie, aperson, passw):
	if not hasMovie(amovie, passw) or not hasPerson(aperson, passw):
		return False
	else:
		if aperson.p_type == "actor":
			table = "ACTING"
			id_type = "A_ID"
		elif aperson.p_type == "director":
			table = "DIRECTING"
			id_type = "D_ID"
		else:
			table = "WRITING"
			id_type = "W_ID"
		movie_id = getMovieID(amovie, passw)
		person_id = getPersonID(aperson, passw)
		conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
		cur = conn.cursor()
		cur.execute("INSERT INTO " + table + "(M_ID, " + id_type + ") VALUES  (" + str(movie_id) + ", " + str(person_id) + ")")
		conn.commit()
		conn.close()
		return True

#def manualAddMovie():
"""
Searches for movies with a matching title.
@params:
	title: string, the title of the movie being searched for
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
@returns:
	an array of tempMovie objects that match the title
	None if no movies are found
"""
def getMovies(title, passw):
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("SELECT * from MOVIES WHERE title = " + "'" + title + "'")
	result = cur.fetchall()
	if result != []:
		found_movies = []
		for row in result:
			cur.execute("SELECT ACTORS.name FROM MOVIES, ACTORS, ACTING WHERE MOVIES.id = " + str(row[0]) + "AND MOVIES.id = ACTING.m_id AND ACTORS.id = ACTING.a_id")
			actors = cur.fetchall()
			cur.execute("SELECT DIRECTORS.name FROM MOVIES, DIRECTORS, DIRECTING WHERE MOVIES.id = " + str(row[0]) + "AND MOVIES.id = DIRECTING.m_id AND DIRECTORS.id = DIRECTING.d_id")
			directors = cur.fetchall()
			cur.execute("SELECT WRITERS.name FROM MOVIES, WRITERS, WRITING WHERE MOVIES.id = " + str(row[0]) + "AND MOVIES.id = WRITING.m_id AND WRITERS.id = WRITING.w_id")
			writers = cur.fetchall()
			movie = tempMovie(row[1], directors, writers, actors, row[2], row[3], row[4], row[5], row[6], row[7])
			found_movies.append(movie)
		return found_movies

"""
Given a person, returns info from the Movies table for all the films they've worked on.
@params:
	person: tempPerson object
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
@returns:
	an array of tuples representing info from the Movies db
"""
def portfolio(person, passw):
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	person_id = getPersonID(person, passw)
	if person.p_type == "actor":
		cur.execute("SELECT TITLE,YEAR,RUNTIME,MPAA,RATING,WATCHED,OWN from MOVIES, ACTORS, ACTING WHERE ACTORS.id = " + str(person_id) + " AND MOVIES.id = ACTING.m_id AND ACTORS.id = ACTING.a_id")
	elif person.p_type == "director":
		cur.execute("SELECT TITLE,YEAR,RUNTIME,MPAA,RATING,WATCHED,OWN from MOVIES, DIRECTORS, DIRECTING WHERE DIRECTORS.id = " + str(person_id) + " AND MOVIES.id = DIRECTING.m_id AND DIRECTORS.id = DIRECTING.a_id")
	else:
		cur.execute("SELECT TITLE,YEAR,RUNTIME,MPAA,RATING,WATCHED,OWN from MOVIES, WRITERS, WRITING WHERE WRITERS.id = " + str(person_id) + " AND MOVIES.id = WRITING.m_id AND WRITERS.id = WRITING.a_id")
	return cur.fetchall()

"""
Checks for a duplicate of the movie object entered.
As of version 1.0, 2 movies with the same year and title cannot exist in the db.
@params:
	h_movie: tempMovie object, movie we are checking the db for.
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
@returns:
	False if no movie in the db matches h_movie
	True if match is found
"""
def hasMovie(h_movie, passw):
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("SELECT * from MOVIES WHERE title = " + "'" + h_movie.title + "' AND year = " +  "'" + h_movie.year + "'")
	result = cur.fetchall()
	if result != []:
		return True
	else: 
		return False

"""
Checks for a duplicate of the person object entered.
As of version 1.0, 2 people with the same name cannot exist in each table.
@params:
	h_person: tempPerson object, person we are checking the db for.
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
@returns:
	False if no person in the db matches h_person
	True if match is found
"""
def hasPerson(h_person, passw):
	if h_person.p_type == "actor":
		table = "ACTORS"
	elif h_person.p_type == "director":
		table = "DIRECTORS"
	else:
		table = "WRITERS"
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("SELECT * from " + table + " WHERE name = '" + h_person.name + "'")
	result = cur.fetchall()
	if result != []:
		return True
	else:
		return False

"""
Finds the id of the desired movie in the MOVIES table.
@params:
	g_movie: tempMovie object
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
@returns:
	int id of the movie if it exists
	None if the movie is not in the db
"""
def getMovieID(g_movie, passw):
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("SELECT id from MOVIES WHERE title = " + "'" + g_movie.title + "' AND year = " +  "'" + g_movie.year + "'")
	result = cur.fetchall()
	if result != []:
		return result[0][0]

"""
Finds the id of the desired person in the ACTORS, DIRECTORS, or WRITERS table.
@params:
	g_person: tempPerson object
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
@returns:
	int id of the person if it exists
	None if the person is not in the db
"""
def getPersonID(g_person, passw):
	if g_person.p_type == "actor":
		table = "ACTORS"
	elif g_person.p_type == "director":
		table = "DIRECTORS"
	else:
		table = "WRITERS"
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("SELECT id from " + table + " WHERE name = '" + g_person.name + "'")
	result = cur.fetchall()
	if result != []:
		return result[0][0]

"""
Mark an existing movie as "Owned"
@params:
	o_movie: tempMovie object representing the movie to be updated
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
"""
def setOwn(o_movie, passw):
	o_id = getMovieID(o_movie, passw)
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("UPDATE MOVIES SET OWN = TRUE WHERE ID = " + str(o_id))
	conn.commit()
	conn.close()

"""
Mark an existing movie as "Unowned"
@params:
	o_movie: tempMovie object representing the movie to be updated
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
"""
def setUnown(o_movie, passw):
	o_id = getMovieID(o_movie, passw)
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("UPDATE MOVIES SET OWN = FALSE WHERE ID = " + str(o_id))
	conn.commit()
	conn.close()

"""
Mark an existing movie as "Watched"
@params:
	w_movie: tempMovie object representing the movie to be updated
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
"""
def setWatched(w_movie, passw):
	w_id = getMovieID(w_movie, passw)
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("UPDATE MOVIES SET WATCHED = TRUE WHERE ID = " + str(w_id))
	conn.commit()
	conn.close()

"""
Mark an existing movie as "Unwatched"
@params:
	w_movie: tempMovie object representing the movie to be updated
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
"""
def setUnwatched(w_movie, passw):
	w_id = getMovieID(w_movie, passw)
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("UPDATE MOVIES SET WATCHED = FALSE WHERE ID = " + str(w_id))
	conn.commit()
	conn.close()
