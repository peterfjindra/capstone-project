#import MySQLdb
from imdb import IMDb
ia = IMDb()

#the_matrix = ia.get_movie('0133093')
#print(the_matrix['director'])

#for person in ia.search_person('Mel Gibson'):
#    print(person.personID, person['name'])
print ia.search_movie("One Flew Over The Cuckoo's Nest")
cuckoos_nest_id = ia.search_movie("One Flew Over The Cuckoo's Nest")[0].movieID
cuckoos_nest = ia.get_movie(cuckoos_nest_id)
cn_dir = cuckoos_nest['director'][0]
#print(cuckoos_nest['writer'])
#print("" + cuckoos_nest['year'])
#print(cuckoos_nest['cast'])
print(cuckoos_nest['mpaa'][0])
#print(cuckoos_nest['countries'])
#print(cuckoos_nest['certificates'])
if cn_dir in cuckoos_nest:
	print "%s is the director of %s" % (cn_dir, cuckoos_nest)
	#"Milos Forman is the director of One Flew Over The Cuckoo's Nest"
	