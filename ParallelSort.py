#!/usr/bin/python2.7
#
# Assignment3 Interface
#
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'

import psycopg2
import os
import sys
import threading 
import itertools as it

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
	num_threads=5
	cur=openconnection.cursor()

	# use the min_max function to get the min and max 
	# value of the  desired column from the Input table
	min_col, max_col=min_max(InputTable,SortingColumnName, cur)

	# Find a step to give equal distributions of values given the SortingColumnsName
	# maximum and minimum. 
	col_range=max_col-min_col
	step=col_range/num_threads

	# find the index of the steps from min_col:max_col
	# given the number of batches you are trying to make based on the number of threads
	# you want to concurrently run 

	interval_lst=[min_col]
	while min_col < max_col:
		min_col+=step
		interval_lst.append(min_col)

	# need to create a thread list so that the started threads can later 
	# iterate over every thread and use join() method to make sure that all 
	# threads finish running before being put into the output table 

	thread_lst=[None]*5

	# a temporary tables will be made for each thread and will consist of the 
	# the data in the range determined by the interval_lst
	temp_table_prefix='temp'

	# start threads which are processing the sorting_table function on different intervals 
	#  These threads will produce temporary tables in which the values of the SortingColumn are 
	# in ascending order 

	for i in range(num_threads):
		table_name=str(temp_table_prefix)+str(i)

		# drop temporary table if it exists 

		drop_table_query='DROP TABLE IF EXISTS {0}'.format(table_name)
		cur.execute(drop_table_query)

		# create the temporary table that is used in the threading function
		create_table_query= """
							CREATE TABLE {0} 
							AS (SELECT * FROM {1} WHERE 1=2)
							""".format(table_name,InputTable)
		cur.execute(create_table_query)
		print(table_name+' created')

		# define a subset of the Input_table using the step indexes contained in the interval_lst.
		# this will be used to split the SortingColumn into tables that have equal ranges given the number of desired threads 
	
		lower_bound, upper_bound=interval_lst[i], interval_lst[i+1]
		
		thread_lst[i]=threading.Thread(target=sorting_tables, args=(InputTable, SortingColumnName, table_name, lower_bound, upper_bound, i, openconnection) )
		thread_lst[i].start()
		print (lower_bound, upper_bound)
		print(thread_lst)

	# create an output table which the user will define, where the temporary tables will be inserted into 

	output_table_query='CREATE TABLE {0} AS (SELECT * FROM {1} WHERE 1=2) '.format(OutputTable,InputTable)
	cur.execute(output_table_query)
	print(OutputTable+ ' was created')

	# close the threads and then append the temp tables to the output table 
	for i in range(num_threads):
		thread_lst[i].join()
		temp_table=temp_table_prefix+str(i)

		cur.execute("INSERT INTO {0} SELECT * FROM {1}".format(OutputTable,temp_table))
		
		#insert_query='INSERT INTO {0} VALUES {1} ;'.format(OutputTable,temp_table)
		#cur.execute(insert_query)
		print (temp_table+' was inserted into '+ OutputTable)
	cur.close()
	openconnection.commit()

	return

def sorting_tables(InputTable, SortingColumnName,table_name, lower_bound, upper_bound, i, openconnection):
	 # sort a given portion of the sorting columns defined as all rows that contain values between the lower and upper boundries 
	# this function will be used in the threading.Thread module to speed up sorting a table by its values.
	# if i==0 then values>= lower bound  included in the table else values > lower_bound are included.
	cur=openconnection.cursor()

	if i==0:
		query='''INSERT INTO {4}
				SELECT * 
				FROM {0} 
				WHERE {1} >= {2} AND {1} <={3}
				ORDER BY {1} ASC 
			'''.format(InputTable, SortingColumnName, lower_bound, upper_bound, table_name)
		cur.execute(query)

	else: 
		query='''
				INSERT INTO {4}
				SELECT * 
				FROM {0}
				WHERE {1} > {2} AND {1} <={3}
				ORDER BY {1} ASC 
			'''.format(InputTable, SortingColumnName, lower_bound, upper_bound,table_name)
		cur.execute(query)




def min_max(InputTable,SortingColumnName, cur):
	#use this function to determine the maximum and minimum values of a desired column

	query='''
			SELECT MIN({0}), MAX({0})
			FROM {1}
			'''.format(SortingColumnName, InputTable)
	cur.execute(query)
	return cur.fetchone()





def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
	# define variables used throughout program. The two temp tables represent the tables in which batches of data from
	# either InputTable1 or InputTable2 be put into their respective temporary table. These 2 temporary tables will then be joined 


	temp_table1_prefix='tmp_tab_parallel'
	temp_table2_prefix='tmp_tab2_parallel'
	join_out_table_prefix='join_out'
	cur=openconnection.cursor()



	query=''' SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{0}';
	'''.format(InputTable2)

	cur.execute(query)
	schema2=cur.fetchall()

	schema2=[item for item in schema2 if item[0]!=Table2JoinColumn]
	print(schema2[0][0]==Table2JoinColumn)


	create_same_schema_table(InputTable1, OutputTable, openconnection)

	for item in schema2:
		query='ALTER TABLE {0} ADD {1} {2}'.format(OutputTable, item[0], item[1]) 
		cur.execute(query)
		print(item[0]+ 'added')


	num_threads=5 

	# find the number of steps required to go from [min_value:max_value step=num_threads]
	min_col, max_col=min_max_parallel(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, cur)
	step_idx=equal_steps(min_col, max_col, num_threads)
	thread_lst=[None]*num_threads
	
	for i in range(num_threads):

		# find the values for the correct interval to extract values from
		lower_bound=step_idx[i]
		upper_bound=step_idx[i+1]

		#create 2 temporary tables for every split in which we will join all the rows from Input and put them in join_out_table
		out_table1=temp_table1_prefix+str(i)
		out_table2=temp_table2_prefix+str(i)
		join_out_table=join_out_table_prefix+str(i)


		create_same_schema_table(InputTable1, out_table1,openconnection)
		create_same_schema_table(InputTable2, out_table2, openconnection)
		create_same_schema_table(OutputTable,join_out_table,openconnection)

		thread_lst[i]=threading.Thread(target=sorting_tables_parallel, args= (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, out_table1, out_table2, join_out_table, lower_bound,  upper_bound,i, openconnection))
		thread_lst[i].start()

		print(thread_lst)


	for i in range(num_threads):
		thread_lst[i].join()
		join_out_table=join_out_table_prefix+str(i)
		
		cur.execute("INSERT INTO {0} SELECT * FROM {1}".format(OutputTable,join_out_table))

		print(join_out_table+ ' was put into '+ OutputTable)

		query='SELECT * FROM {0}'.format(OutputTable)
		cur.execute(query)
		


	cur.close()
	openconnection.commit()






def sorting_tables_parallel(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, out_table1, out_table2, join_out_table,lower_bound, upper_bound, i, openconnection):

	cur=openconnection.cursor()
	query=''' SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{0}';
	'''.format(InputTable2)

	cur.execute(query)
	schema2=cur.fetchall()
	schema2=[item[0] for item in schema2 if item[0]!=Table1JoinColumn]

	out_col=[out_table2+'.'+item for item in schema2]
	out_col_str=','.join(out_col)



	if i==0:

		query='''INSERT INTO {0}
				SELECT * 
				FROM {1} 
				WHERE {2} >= {3} AND {2} <={4} ;
			'''.format(out_table1, InputTable1,Table1JoinColumn,lower_bound, upper_bound  )
		cur.execute(query)
		#print('data from '+str(lower_bound) +' '+ str(upper_bound)+ ' was inserted into '+ out_table1  )

		query='''INSERT INTO {0}
				SELECT * 
				FROM {1} 
				WHERE {2} >= {3} AND {2} <={4} ;
			'''.format(out_table2, InputTable2,Table2JoinColumn,lower_bound, upper_bound  )
		cur.execute(query)

		#print('data from '+str(lower_bound) +' '+ str(upper_bound)+ ' was inserted into '+ out_table2 )

	else: 
		query='''INSERT INTO {0}
				SELECT * 
				FROM {1} 
				WHERE {2} > {3} AND {2} <={4} ;
			'''.format(out_table1, InputTable1,Table1JoinColumn,lower_bound, upper_bound  )
		cur.execute(query)
		#print('data from '+str(lower_bound) +' to '+ str(upper_bound)+ ' was inserted into '+ out_table1  )



		query='''INSERT INTO {0}
				SELECT * 
				FROM {1} 
				WHERE {2} > {3} AND {2} <={4} ;
			'''.format(out_table2, InputTable2,Table2JoinColumn,lower_bound, upper_bound  )
		cur.execute(query)

	query='''
	INSERT INTO {5}
	SELECT {0}.*, {4}
	FROM {0} ,{1} 
	WHERE {0}.{2}={1}.{3}
	'''.format(out_table1, out_table2, Table1JoinColumn, Table2JoinColumn,out_col_str,join_out_table)
	cur.execute(query)

  


def min_max_parallel(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, cur):

	query='''
	(SELECT {0} FROM {1})
	UNION 
	(SELECT {2} FROM {3})

	'''.format(Table1JoinColumn, InputTable1, Table2JoinColumn, InputTable2)
	cur.execute(query)

	column_union=[item[0] for item in cur.fetchall()] 
	max_col=max(column_union)
	min_col=min(column_union)

	return min_col, max_col



def create_same_schema_table(input_table, output_table, openconnection):
	cur=openconnection.cursor()

	query='''
		CREATE TABLE {0}
		AS (SELECT * FROM {1} WHERE 1=2)'''.format(output_table, input_table)
	cur.execute(query)
	#print(output_table+' was created')


def equal_steps(min_col, max_col, num_threads):

	index_steper=it.count(start=min_col, step=(max_col-min_col) /num_threads)
	return [next(index_steper) for _ in range(num_threads+1)]



def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
	return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")










 
################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
	return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment'):
	"""
	We create a DB by connecting to the default user and database of Postgres
	The function first checks if an existing database exists for a given name, else creates it.
	:return:None
	"""
	# Connect to the default database
	con = getOpenConnection(dbname='postgres')
	con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
	cur = con.cursor()

	# Check if an existing database with the same name exists
	cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
	count = cur.fetchone()[0]
	if count == 0:
		cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
	else:
		print 'A database named {0} already exists'.format(dbname)

	# Clean up
	cur.close()
	con.commit()
	con.close()



