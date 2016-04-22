import psycopg2
from db_personal import *
from temp_objects import *
from getpass import getpass

passw = getpass("Please enter the password for your postgres account:")
movie1 = tempMovie("The Lion King", None, None, None, "1994", 95, "PG", 6.0, True, False)
person = tempPerson("Matthew Broderick", "actor")
movie2 = tempMovie("Ferris Buellers Day Off", None, None, None, "1986", 103, "PG-13", 8.5, True, True)
movie3 = tempMovie("Resevoir Dogs", None, None, None, "1992", 99, "R", 9.5, True, True)

#addMovie(movie, passw)
#addMovie(movie2, passw)
#addMovie(movie3, passw)
#addPerson(person, passw)
#addRole(movie2, person, passw)
#addRole(movie1, person, passw)
"""
portfolio = portfolio(person, passw)
for movie in portfolio:
    print movie[0] + ", " + movie[1] + ", " + str(movie[2]) + ", " + movie[3] + ", " + str(movie[4]) + ", " + str(movie[5] + "," + str(movie[6]))
"""
#setUnown(movie1, passw)
#setWatched(movie2, passw)

"""
lionkings = getMovies("The Lion King", passw)
lionking = lionkings[0]
print lionking.title
print lionking.director
print lionking.writer
print lionking.cast
print lionking.year 
print lionking.runtime 
print lionking.rating 
print lionking.watched
"""
#conn = psycopg2.connect(database="test", user="postgres", password=passw, host="127.0.0.1", port="5432")
#print "Opened database successfully"

#CREATE TABLE
"""
cur = conn.cursor()
cur.execute('''CREATE TABLE COMPANY
       (ID INT PRIMARY KEY     NOT NULL,
       NAME           TEXT    NOT NULL,
       AGE            INT     NOT NULL,
       ADDRESS        CHAR(50),
       SALARY         REAL);''')
print "Table created successfully"

conn.commit()
conn.close()
"""
#INSERT
"""
cur = conn.cursor()
cur.execute("INSERT INTO MOVIES (TITLE,YEAR,RUNTIME,MPAA,RATING) \
      VALUES ('Kung Fury', '2015', 30, 'R', 8.0)");
conn.commit()
conn.close()

cur.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (2, 'Allen', 25, 'Texas', 15000.00 )");

cur.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (3, 'Teddy', 23, 'Norway', 20000.00 )");

cur.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 )");

conn.commit()
print "Records created successfully";
conn.close()
"""
#SELECT
"""
cur = conn.cursor()

cur.execute("SELECT id, name, address, salary  from COMPANY")
rows = cur.fetchall()
for row in rows:
   print "ID = ", row[0]
   print "NAME = ", row[1]
   print "ADDRESS = ", row[2]
   print "SALARY = ", row[3], "\n"

print "Operation done successfully";
conn.close()
"""
#UPDATE
"""
cur = conn.cursor()

cur.execute("UPDATE COMPANY set SALARY = 25000.00 where ID=1")
conn.commit
print "Total number of rows updated :", cur.rowcount

cur.execute("SELECT id, name, address, salary  from COMPANY")
rows = cur.fetchall()
for row in rows:
   print "ID = ", row[0]
   print "NAME = ", row[1]
   print "ADDRESS = ", row[2]
   print "SALARY = ", row[3], "\n"

print "Operation done successfully";
conn.close()
"""
#DELETE
"""
cur = conn.cursor()

cur.execute("DELETE from COMPANY where ID=2;")
conn.commit
print "Total number of rows deleted :", cur.rowcount

cur.execute("SELECT id, name, address, salary  from COMPANY")
rows = cur.fetchall()
for row in rows:
   print "ID = ", row[0]
   print "NAME = ", row[1]
   print "ADDRESS = ", row[2]
   print "SALARY = ", row[3], "\n"

print "Operation done successfully";
conn.close()
"""