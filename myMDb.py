"""
myMDb.py
language: python2
author: Peter Jindra, peterfjindra@gmail.com

Main page for the myMDb application.
This application utilizes a PostgreSQL database to store information about movies for the user.
The IMBbPY python package is used to get data from IMDb, which is stored in a local database.
More info on how this application works can be found in the function header comments and the README file.
To run the application, just run this python program (after installing PostgreSQL and IMDbPY)!

This code is publicly available on https://github.com/peterfjindra/capstone-project.git
IMDbPY package is hosted on: http://imdbpy.sourceforge.net/
myMDb is a working title and is not to be confused with many other lazily-named projects that probably do the same thing: http://lmgtfy.com/?q=mymdb
Special 'thank you' to www.imdb.com for keeping all this info available so we don't have to (although you can manually enter movies if you want) 

[Small note to anyone actually reading the comments: I refer to 'Python lists', aka [], as 'arrays'. I just find it more useful to use this
terminology, since more people are probably familiar with it.] 
"""

import sys
import db_setup
from db_web import *
from db_personal import *
from temp_objects import tempMovie, tempPerson
from getpass import getpass

"""
A preliminary check to see if the db exists.
@params:
	passw: string, the password entered by the user
@returns:
	True if the database exists
	False if the password doesn't exist
"""
def dbExists(passw):
	try:
		psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
		return True
	except psycopg2.OperationalError:
		return False

"""
Checks for the correct password by attempting to connect to the default db.
@params:
	passw: string, the password entered by the user
@returns:
	True if the password is correct
	False if the password is incorrect
"""
def correctPass(passw):
	try:
		psycopg2.connect(database="postgres", user="postgres", password=passw, host="127.0.0.1", port="5432")
		return True
	except psycopg2.OperationalError:
		return False

def viewStuff(passw):
	while(1):
		print "\nWhat would you like to do?"
		print "(M)ovie search."
		print "(C)heck if a specific actor, director, or writer is in my database."
		print "(P)ortfolio display of an actor, director, or writer in my database."
		print "(L)ist of movies in my database I haven't watched."
		print "(E)xit to the main menu."
		answer = raw_input(":").upper()
		if answer.upper() == "M":
			print "What's the title of the movie you want to search for?"
			title = raw_input(":").upper().replace("'","")
			found_movies = getMovies(title, passw)
			if found_movies == None:
				print "No movies in the database by that title."
				continue
			else:
				print "\nWe found " + str(len(found_movies)) + " movie(s) with that title in the db."
				for movie in found_movies:
					movie.printInfo()
		elif answer == "C":
			print "Are you searching for an (A)ctor, (D)irector, or (W)riter?"
			type_input = raw_input(":")
			if type_input.upper() == "A":
				p_type = "actor"
			elif type_input.upper() == "D":
				p_type = "director"
			elif type_input.upper() == "W":
				p_type = "writer"
			else:
				print "Incorrect type. Valid inputs are A, D, and W."
				continue
			print "What's the name of the person you're searching for?"
			name = raw_input(":").upper().replace("'","")
			if hasPerson(tempPerson(name, p_type), passw):
				print "\n" + name + " is in your database."
			else:
				print name + " is not in your database."
			continue
		elif answer == "P":
			print "\nAre you searching for an (A)ctor, (D)irector, or (W)riter?"
			type_input = raw_input(":")
			if type_input.upper() == "A":
				p_type = "actor"
			elif type_input.upper() == "D":
				p_type = "director"
			elif type_input.upper() == "W":
				p_type = "writer"
			else:
				print "Incorrect type. Valid inputs are A, D, and W."
				continue
			print "What's the name of the person you're searching for?"
			name = raw_input(":").upper()
			results = portfolio(tempPerson(name, p_type), passw)
			if portfolio:
				print "\n" + name + " worked on these movies in your database:"
				for movie in results:
					print movie.simpleToString()
				continue
			else:
				print name + " is not in your database."
		elif answer == "L":
			unwatched_movies = getMoviesToWatch(passw)
			print "\nMovies To Watch:\n"
			for movie in unwatched_movies:
				print movie.simpleToString()
			continue
		elif answer == "E":
			break
		else:
			print "Please enter one of the suggested options."
			continue

def addStuff(passw):
	while(1):
		print "\nWhat would you like to do?"
		print "(A)dd a movie to the database."
		print "(U)pdate a movie in the database."
		print "(E)xit to the main menu."
		answer = raw_input(":").upper()
		if answer == "E":
			break
		elif answer == "A":
			print "\nWhat is the title of the movie you want to add?"
			desired_title = raw_input(":")
			pulled_movie = pullMovie(desired_title, False)
			if not pulled_movie:
				break
			rating, watched, own = userSpecificInfo()
			pulled_movie.rating = rating
			pulled_movie.watched = watched
			pulled_movie.own = own
			if not addMovie(pulled_movie, passw):
				print "This movie is already in the database."
			else:
				print "Adding info to the database. This may take a moment..."
				for director in pulled_movie.director:
					new_director = tempPerson(director.upper(), 'director')
					addPerson(new_director, passw)
					addRole(pulled_movie, new_director, passw)
				for writer in pulled_movie.writer:
					new_writer = tempPerson(writer.upper(), 'writer')
					addPerson(new_writer, passw)
					addRole(pulled_movie, new_writer, passw)
				for actor in pulled_movie.cast:
					new_actor = tempPerson(actor.upper(), 'actor')
					addPerson(new_actor, passw)
					addRole(pulled_movie, new_actor, passw)
				print "The movie and the people associated with it have been added to the db."
				continue
		elif answer == "U":
			print "\nWhat's the title of the movie you want to search for?"
			title = raw_input(":").upper().replace("'","")
			found_movies = getMovies(title, passw)
			if found_movies == None:
				print "No movies in the database by that name."
				continue
			else:
				selection = chooseResult(None, found_movies, True)
				if selection:
					print "OK. Lets update " + selection.title
					rating, watched, own = userSpecificInfo()
					setRating(selection, rating, passw)
					setOwn(selection, own, passw)
					setWatched(selection, watched, passw)
			continue
		else:
			print "Invalid input."
			continue					

def userSpecificInfo():
	while(1):
		print "What would you rate this movie? (Type 'N' if you don't want to rate it.)"
		rating = raw_input(":").upper()
		try:
			float(rating)
		except ValueError:
			if rating == "N":
				rating = "n/a"
			else:
				print "Type a number or the letter 'N'."
				continue
		break
	while(1):
		print "Have you seen this movie? Y/N."
		watched = raw_input(":").upper()
		if watched == "Y":
			watched = True
		elif watched == "N":
			watched = False
		else:
			print "Invalid input."
			continue
		break
	while(1):
		print "Do you own this movie? Y/N."
		own = raw_input(":").upper()
		if own == "Y":
			own = True
		elif own == "N":
			own = False
		else:
			print "Invalid input."
			continue
		break
	return rating, watched, own

"""
The main menu of the application. Splits up duties between updating db and viewing db.
Loop keeps the app running until the user wants to quit.
@params:
	passw: string, the password to access the db carried over so the user doesn't have to enter it again
"""
def mainMenu(passw):
	while(1):
		print "\nWhat would you like to do?"
		print "(A)dd or update movies."
		print "(V)iew information in the database."
		print "(Q)uit"
		answer = raw_input(":")
		if answer.upper() == "A":
			addStuff(passw)
		elif answer.upper() == "V":
			viewStuff(passw)
		elif answer.upper() == "Q":
			break
		else:
			print "Please enter one of the suggested options."
			continue

def main():
	print "Welcome to myMDb!"
	passw = getpass("To begin, please enter your PostgreSQL password:")

	#first, we make sure they entered the correct password
	attempts = 0
	while not correctPass(passw):
		attempts += 1
		if attempts % 3 == 0:
			print "Unable to connect to the db. Make sure you have PostgreSQL installed correctly."
		passw = getpass("Incorrect password. Please try again:")

	#if this is their first time using the app, we need to create the database
	if not dbExists(passw):
		print "Welcome, first time user! Please give me a moment to set things up."
		db_setup.createDb(passw)

	mainMenu(passw)

	sys.exit("Thanks for using myMDb!")		

if __name__ == "__main__":
	main()
