import psycopg2
import itertools as it
import threading

FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
	# define variables used throughout program. The two temp tables represent the tables in which batches of data from
	# either InputTable1 or InputTable2 be put into their respective temporary table. These 2 temporary tables will then be joined 


	temp_table1_prefix='tmp_table1_'
	temp_table2_prefix='tmp_table2_'
	join_out_table_prefix='join_out'
	cur=openconnection.cursor()

	num_threads=5 

	# find the number of steps required to go from [min_value:max_value step=num_threads]
	min_col, max_col=min_max(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, cur)
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

		thread_lst[i]=threading.Thread(target=sorting_tables_parallel, args= (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, out_table1, out_table2, join_out_table, lower_bound,  upper_bound,i, openconnection))
		thread_lst[i].start()
		#print(thread_lst)

	for i in range(num_threads):
		join_out_table=join_out_table_prefix+str(i)
		create_same_schema_table(join_out_table, OutputTable, openconnection)
		thread_lst[i].join()
		cur.execute("INSERT INTO {0} SELECT * FROM {1}".format(OutputTable,join_out_table))

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

        query='''

        CREATE TABLE {5} AS 
        SELECT {0}.*, {4}
        FROM {0} ,{1} 
        WHERE {0}.{2}={1}.{3}
        '''.format(out_table1, out_table2, Table1JoinColumn, Table2JoinColumn,out_col_str,join_out_table)
        cur.execute(query)
        print(cur.fetchone())


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

        CREATE TABLE {5} AS 
        SELECT {0}.*, {4}
        FROM {0} ,{1} 
        WHERE {0}.{2}={1}.{3}
        '''.format(out_table1, out_table2, Table1JoinColumn, Table2JoinColumn,out_col_str,join_out_table)
        cur.execute(query)
        print(cur.fetchone())      






def min_max(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, cur):

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


conn=getOpenConnection()




ParallelJoin ('range_part0', 'range_part1', 'userid', 'userid', 'asd', conn)


#create_same_schema_table('range_part0', 'test', conn)







	




