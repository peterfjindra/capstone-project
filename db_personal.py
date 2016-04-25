"""
db_personal.py
language: python2
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
	passw:      string, the password to access the db carried over so the user doesn't have to enter it again
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
	passw:     string, the password to access the db carried over so the user doesn't have to enter it again
@return:
	True if the movie was added
	False if the movie already existed
"""
def addMovie(new_movie, passw):
	if hasMovie(new_movie, passw):
		return False
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	values = "('" + new_movie.title + "','" + new_movie.year + "','" + new_movie.runtime + "','" + new_movie.mpaa + \
		    "','" + new_movie.rating + "'," + str(new_movie.watched) + "," + str(new_movie.own) + ")"
	#print "INSERT INTO MOVIES (TITLE,YEAR,RUNTIME,MPAA,RATING) VALUES " + values
	cur.execute("INSERT INTO MOVIES (TITLE,YEAR,RUNTIME,MPAA,RATING,WATCHED,OWN) VALUES " + values)
	conn.commit()
	conn.close()
	return True

"""
Adds foreign keys for a person and movie to ACTING, DIRECTING, or WRITING
@params:
	amovie:  tempMovie object
	aperson: tempPerson object
	passw:   string, the password to access the db carried over so the user doesn't have to enter it again
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
			cur.execute("SELECT ACTORS.name FROM MOVIES, ACTORS, ACTING WHERE MOVIES.id = " + str(row[0]) + " AND MOVIES.id = ACTING.m_id AND ACTORS.id = ACTING.a_id")
			actors = []
			for entry in cur.fetchall():
				actors.append(entry[0])
			cur.execute("SELECT DIRECTORS.name FROM MOVIES, DIRECTORS, DIRECTING WHERE MOVIES.id = " + str(row[0]) + " AND MOVIES.id = DIRECTING.m_id AND DIRECTORS.id = DIRECTING.d_id")
			directors = []
			for entry in cur.fetchall():
				directors.append(entry[0])
			cur.execute("SELECT WRITERS.name FROM MOVIES, WRITERS, WRITING WHERE MOVIES.id = " + str(row[0]) + " AND MOVIES.id = WRITING.m_id AND WRITERS.id = WRITING.w_id")
			writers = []
			for entry in cur.fetchall():
				writers.append(entry[0])			
			movie = tempMovie(row[1], directors, writers, actors, row[2], row[3], row[4], row[5], row[6], row[7])
			found_movies.append(movie)
		return found_movies

"""
Given a person, returns info from the Movies table for all the films they've worked on.
@params:
	person: tempPerson object
	passw:  string, the password to access the db carried over so the user doesn't have to enter it again
@returns:
	an array of tempMovie objects where the people categories are 'None'
	None if the actor does not exist in the database
"""
def portfolio(person, passw):
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	person_id = getPersonID(person, passw)
	if person_id == None:
		return None
	if person.p_type == "actor":
		cur.execute("SELECT TITLE,YEAR,RUNTIME,MPAA,RATING,WATCHED,OWN from MOVIES, ACTORS, ACTING WHERE ACTORS.id = " + str(person_id) + " AND MOVIES.id = ACTING.m_id AND ACTORS.id = ACTING.a_id")
	elif person.p_type == "director":
		cur.execute("SELECT TITLE,YEAR,RUNTIME,MPAA,RATING,WATCHED,OWN from MOVIES, DIRECTORS, DIRECTING WHERE DIRECTORS.id = " + str(person_id) + " AND MOVIES.id = DIRECTING.m_id AND DIRECTORS.id = DIRECTING.d_id")
	else:
		cur.execute("SELECT TITLE,YEAR,RUNTIME,MPAA,RATING,WATCHED,OWN from MOVIES, WRITERS, WRITING WHERE WRITERS.id = " + str(person_id) + " AND MOVIES.id = WRITING.m_id AND WRITERS.id = WRITING.w_id")
	found_movies = []
	for row in cur.fetchall():
		found_movies.append(tempMovie(row[0], None, None, None, row[1], str(row[2]), row[3], str(row[4]), str(row[5]), str(row[6])))
	return found_movies

"""
Returns all movies that the user hasn't watched.
@params:
	passw:  string, the password to access the db carried over so the user doesn't have to enter it again
@returns:
	an array of tempMovie objects 
"""
def getMoviesToWatch(passw):
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("SELECT TITLE,YEAR,RUNTIME,MPAA,RATING,WATCHED,OWN from MOVIES WHERE MOVIES.watched = FALSE")
	found_movies = []
	for row in cur.fetchall():
		found_movies.append(tempMovie(row[0], None, None, None, row[1], str(row[2]), row[3], str(row[4]), str(row[5]), str(row[6])))
	return found_movies

"""
Checks for a duplicate of the movie object entered.
As of version 1.0, 2 movies with the same year and title cannot exist in the db.
@params:
	h_movie: tempMovie object, movie we are checking the db for.
	passw:   string, the password to access the db carried over so the user doesn't have to enter it again
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
	passw:    string, the password to access the db carried over so the user doesn't have to enter it again
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
	passw:   string, the password to access the db carried over so the user doesn't have to enter it again
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
	passw:    string, the password to access the db carried over so the user doesn't have to enter it again
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
Change/Add a rating to an existing movie in the database.
@params:
	r_movie: tempMovie object representing the movie to be updated
	passw:   string, the password to access the db carried over so the user doesn't have to enter it again
	rating:  float, the rating to give to the movie 
"""
def setRating(r_movie, rating, passw):
	r_id = getMovieID(r_movie, passw)
	r_id = getMovieID(r_movie, passw)
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("UPDATE MOVIES SET RATING = " + rating + " WHERE ID = " + str(r_id))
	conn.commit()
	conn.close()

"""
Mark an existing movie as "Owned"
@params:
	o_movie: tempMovie object representing the movie to be updated
	own:     boolean, what to set the value of 'own' to
	passw:   string, the password to access the db carried over so the user doesn't have to enter it again
"""
def setOwn(o_movie, own, passw):
	if own:
		own_string = "TRUE"
	else:
		own_string = "FALSE"
	o_id = getMovieID(o_movie, passw)
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("UPDATE MOVIES SET OWN = " + own_string + " WHERE ID = " + str(o_id))
	conn.commit()
	conn.close()

"""
Mark an existing movie as "Watched"
@params:
	w_movie: tempMovie object representing the movie to be updated
	passw:   string, the password to access the db carried over so the user doesn't have to enter it again
"""
def setWatched(w_movie, watched, passw):
	if watched:
		watched_string = "TRUE"
	else:
		watched_string = "FALSE"
	w_id = getMovieID(w_movie, passw)
	conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
	cur = conn.cursor()
	cur.execute("UPDATE MOVIES SET WATCHED = " + watched_string + " WHERE ID = " + str(w_id))
	conn.commit()
	conn.close()