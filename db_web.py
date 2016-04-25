"""
db_web.py
language: python2
author: Peter Jindra, peterfjindra@gmail.com

A series of functions used for myMDb project.
These functions specifically use the IMDbPY python package to get information about movies from IMDb.
The package was not created by me, is open-source, and can be found at http://imdbpy.sourceforge.net/
"""
from imdb import IMDb
from temp_objects import *

"""
A function which gets info from IMDb, and queries you to select the correct movie to add to your personal db.

Many of the IMDbPy queries don't have every field, so this function does some awkward error handling
to stick a python null type, called 'None', into places where trying to extract the information causes a KeyError.
For example, if a movie doesn't have a year listed, asking the IMDbPy movie object for the year causes an error, instead
of returning an empty string or Nonetype. When this happens, the code below will put a 'None' into the year field so that
the other sections of the code can recognize there is nothing there.

When this function is first run, it will grab the top 5 entries from the search results of the title it is given.
This is done to save time. It builds tempMovie objects (defined in temp_objects.py of this project) for each one, and displays some basic info 
so the user can choose which title he wants. Alternatively, the user can choose to run this function again, this time returning ALL the search 
results, which takes quite a bit longer.

@params:
	title:    string, the title that the user is searching for.
	grab_all: boolean, False if only grabbing the top 5 results, True if grabbing all results. 
@returns:
	False if the user decides to exit without choosing a movie
	otherwise, a tempMovie object selected by the user
"""
def pullMovie(s_title, grab_all):
	ia = IMDb()
	if grab_all:
		print "\nGetting lots of information from IMDb. Please be patient..."
		try:
			id_list = ia.search_movie(s_title)[:20]
		except IndexError:
			id_list = ia.search_movie(s_title)
	else:
		print "\nGetting information from IMDb. This may take a moment..."
		try:
			id_list = ia.search_movie(s_title)[:3]
		except IndexError:
			id_list = ia.search_movie(s_title)			
	mymovie_objs = []
	imdbpy_objs = []
	for basic_movie in id_list:
		imdbpy_objs.append(ia.get_movie(basic_movie.movieID))
	for imdbpy_obj in imdbpy_objs:
		runtime = "n/a"
		try:	
			if len(imdbpy_obj['runtimes']) == 1:
				runtime = imdbpy_obj['runtimes'][0]
			else:
				for time in imdbpy_obj['runtimes']:
					if "USA" in time:
						runtime = time[4:]
		except KeyError:
			runtime = "n/a"
		try:
			for rating in imdbpy_obj['certificates']:
				if "USA" in rating:
					mpaa = rating[4:].split(":")[0]
		except KeyError:
			mpaa = "n/a"
		try:
			title = imdbpy_obj['title'].replace("'","").upper()	
		except KeyError:
			title = "n/a"
		try:
			director = []  
			for person in imdbpy_obj['director']:
				director.append(person['name'].replace("'",""))
		except KeyError:
			director = None
		try:
			writer = []  
			for person in imdbpy_obj['writer']:
				writer.append(person['name'].replace("'",""))
		except KeyError:
			writer = None
		try:
			cast = []  
			for person in imdbpy_obj['cast']:
				cast.append(person['name'].replace("'",""))
		except KeyError:
			cast = None
		try:
			year = str(imdbpy_obj['year'])
		except KeyError:
			year = None
		
		mymovie_objs.append(tempMovie(title, director, writer, cast, year, runtime, mpaa, None, None, None))
	
	return chooseResult(s_title, mymovie_objs, grab_all)
	

"""
A helper function for pullMovies(). Displays found movies and asks the user to make a decision.
@params:
	mymovie_objs: array of tempMovie objects, used to hold the information for potential movies to be added
	grab_all:     boolean, False if only grabbing the top 5 results, True if grabbing all results. 
@returns:
	the users input, either an int representing their selection, 'A' for advanced search, or 'E' for exit.
"""
def chooseResult(s_title, mymovie_objs, grab_all):
	count = 1
	for movie_obj in mymovie_objs:
		try:
			print str(count) + ". " + movie_obj.simpleToString()
			count += 1
		except UnicodeEncodeError:
			print str(count) + " (Error: this result could not be displayed)"			
	print "\nIs one of the above titles what you're looking for?"
	print "(#) Select this title."
	if not grab_all:
		print "(A)dvanced search. Grabs 20 titles that match the query, instead of just 3."
	#print (M)anually enter the desired title. This feature will hopefully be ready for version 1.0
	print "(E)xit this search."
	answer = raw_input(":").upper()
	if answer.isdigit():
		if  0 < int(answer) < (count + 1):
			return mymovie_objs[int(answer) - 1]
		else:
			print "Number out of range."
			chooseResult(mymovie_objs, grab_all)
	elif not grab_all and answer == "A":
		return pullMovie(s_title, True)
	elif answer == "E":
		return False
	else:
		print "Not a valid input."
		chooseResult(s_title, mymovie_objs, grab_all)