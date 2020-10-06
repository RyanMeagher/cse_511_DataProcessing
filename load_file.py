
import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


conn=getOpenConnection()
cur=conn.cursor()





filepath='\'/Users/owner/Desktop/cse_511/movies.dat\''
query='''
		COPY movies 
		FROM {0} DELIMITER '%' CSV'''.format(filepath)


cur.execute(query)

query='SELECT movieid FROM movies'
cur.execute(query)





query ='''CREATE TABLE hasagenre(movieid int,
					   genreid int ); '''
cur.execute(query)


f='\'/Users/owner/Desktop/cse_511/hasagenre.dat\''
query='''
		COPY hasagenre 
		FROM {0} DELIMITER '%' CSV'''.format(f)


cur.execute(query)
conn.commit()


