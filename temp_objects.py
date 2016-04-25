"""
temp_objects.py
language: python2
author: Peter Jindra, peterfjindra@gmail.com

Classes that are part of the myMDb project.
Mainly, these objects are used for temporary data storage.
"""

"""
Class which temporarily holds the data of a film.
@params:
	title:    string
	director: array of strings, there may be multiple directors
	writer:   (see director)
	cast:     (see director)
	year:     string, year(s) of release. ("XXXX" for movies, "XXXX-XXXX" for TV shows)
	runtime:  string, runtime in minutes
	mppa:     string, mpaa rating (G, PG, PG-13, R, X)
	rating:   float, your personal 1 to 10 rating
	watched:  boolean, True if you've seen the movie
	own:      boolean, True if you own the movie
"""
class tempMovie:
	def __init__(self, title, director, writer, cast, year, runtime, mpaa, rating, watched, own):
		self.title = title
		self.director = director
		self.writer = writer
		self.cast = cast
		self.year = year
		self.runtime = runtime
		self.mpaa = mpaa
		self.rating = rating
		self.watched = watched
		self.own = own

	"""
	Neatly prints all the info about the movie.
	"""
	def printInfo(self):
		print self.title
		try:
			print "Director(s):" + ",".join(self.director)
		except TypeError:
			print "Director(s): n/a"
		try:
			print "Writer(s):" + ",".join(self.writer)
		except TypeError:
			print "Writer(s): n/a"
		try:
			print "Cast:" + ",".join(self.cast)
		except TypeError:
			print "Cast: n/a"
		if self.year == None:
			year = "n/a"
		else:
			year = self.year
		if self.mpaa == None:
			mpaa = "n/a"
		else:
			mpaa = self.mpaa
		if self.runtime == None:
			runtime = "n/a"
		else:
			runtime = self.runtime
		if self.rating == None:
			rating = "n/a"
		else:
			rating = self.rating
		print year + ", " + runtime + " minutes, " + mpaa
		print "You gave this movie a(n) " + str(rating)
		if self.watched:
			print "You have seen this movie."
		else: 
			print "You have not seen this movie."
		if self.own:
			print "You own this movie."
		else:
			print "You don't own this movie."

	"""
	Returns a string of its basic info.
	Mainly used to show portfolio of a person.
	"""
	def simpleToString(self):
		if self.watched == "TRUE":
			w_string = "Watched: YES"
		else:
			w_string = "Watched: NO"
		if self.own == "TRUE":
			o_string = "Own: YES"
		else:
			o_string = "Own: NO"
		if self.title == None:
			title = "n/a"
		else:
			title = self.title
		if self.year == None:
			year = "n/a"
		else:
			year = self.year
		if self.mpaa == None:
			mpaa = "n/a"
		else:
			mpaa = self.mpaa
		if self.runtime == None:
			runtime = "n/a"
		else:
			runtime = self.runtime
		if self.rating == None:
			rating = "n/a"
		else:
			rating = self.rating
		return title + ", " + year + ", " + runtime + " minutes, " + mpaa + ", " + str(rating) + ", " + w_string + ", " + o_string

"""
Class which temporarily holds the data of a person.
@params:
	name:   string
	p_type: string, exclusively "actor", "director", or "writer"
"""
class tempPerson:
	def __init__(self, name, p_type):
		self.name = name
		self.p_type = p_type