"""
temp_objects.py
language: python
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
	runtime:  int, runtime in minutes
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
Class which temporarily holds the data of a person.
@params:
	name:   string
	p_type: string, exclusively "actor", "director", or "writer"
"""
class tempPerson:
	def __init__(self, name, p_type):
		self.name = name
		self.p_type = p_type