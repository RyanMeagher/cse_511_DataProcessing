#!/usr/bin/python2.7
#
# Interface for the assignement
DATABASE_NAME = 'dds_assignment'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

import psycopg2
import os
import io

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):

    #FIRST STEP PREPROCESSING OF FILE ->  change_csvfile
    #csv_file column structure = UserID::MovieID::Rating::Timestamp
    # delimiter='::' When trying to use psycopg copy_from function the file must have a delimiter that is only 1 byte. 
    # only columns [0:3] are kept

    def change_csv(filepath, delimiter,out_delimiter):
        csv_string=''
        with open(filepath,'r') as infile:
            for line in infile:
                line=line.split(delimiter)
                line=(out_delimiter.join([str(i) for i in line[0:3]]))+'\n'
                csv_string+=line 
    
        return csv_string 

    out=change_csv(ratingsfilepath,'::',':')


    # Need to create the ratings table defined with the name 'ratingstablename' UserID:MovieID:Rating

    def CreateRatingsTable(table_name,cursor):
        try:
            query=  ("""
                        CREATE TABLE {0} ({1} int , {2} int , 
                        {3} float(1) CHECK (rating>=0) CHECK (rating<=5) );
                    """).format(table_name,USER_ID_COLNAME,MOVIE_ID_COLNAME,RATING_COLNAME)

            cursor.execute(query)
            print('table created')

        except Exception as e:
            print( 'table not created', e)

   

    with openconnection.cursor() as cur:
        CreateRatingsTable(ratingstablename,cur)

        csv_file=io.BytesIO(out)
        csv_file.seek(0)

        try:
            sqlstr = "COPY {0} FROM STDIN DELIMITER ':' CSV".format(ratingstablename)
            cur.copy_expert(sqlstr, csv_file)
            openconnection.commit()
            print ('data inserted ')
        except Exception as e:
            print(e)

    return


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    lower_lim, upper_lim=(0.0,5.0)
    step=upper_lim/numberofpartitions

    with openconnection.cursor() as cur:
        for i in range(numberofpartitions):
            lower_lim=i*step
            upper_lim=lower_lim+step 
            table_name=RANGE_TABLE_PREFIX+str(i)
            
            create_table_query=  ("""
                        CREATE TABLE {0} ({1} int , {2} int , 
                        {3} float(1) CHECK (rating>=0) CHECK (rating<=5) );
                    """).format(table_name,USER_ID_COLNAME,MOVIE_ID_COLNAME,RATING_COLNAME)
            print(table_name+'was created')





            cur.execute(create_table_query)
            if i==0:
                query=("""
                    INSERT INTO {0} ({1},{2},{3}) 
                    SELECT {1},{2},{3}
                    FROM {6}
                    WHERE {3}>= {4} AND {3}<= {5} 
                    """).format(table_name,USER_ID_COLNAME,MOVIE_ID_COLNAME,RATING_COLNAME, str(lower_lim), str(upper_lim),ratingstablename)
                cur.execute(query)
                print(table_name+'split successful')


            else:
                query=("""
                    INSERT INTO {0} ({1},{2},{3}) 
                    SELECT {1},{2},{3}
                    FROM {6}
                    WHERE {3}> {4} AND {3}<= {5} 
                    """).format(table_name,USER_ID_COLNAME,MOVIE_ID_COLNAME,RATING_COLNAME, str(lower_lim), str(upper_lim),ratingstablename)
                
                cur.execute(query)
                print(table_name+'split successful')
        openconnection.commit()

    return




def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    with openconnection.cursor() as cur:
        
        # determine how many samples need to bedivided into equal partitions

        cur.execute('SELECT * FROM {0} ;'.format(ratingstablename) )
        data= cur.fetchall()

        def partition(lst,n):
            return [lst[i::n] for i in range(n)]

        partitions=partition(data, numberofpartitions)
        table_num=0
        for partition in partitions:
 

            table_name=RROBIN_TABLE_PREFIX+str(table_num)

            create_table_query="""
                                CREATE TABLE {0} ({1} int , {2} int , {3} float(1) );
                                """.format(table_name,USER_ID_COLNAME,MOVIE_ID_COLNAME,RATING_COLNAME)
            cur.execute(create_table_query)
            print(table_name+' was created')

            table_num+=1

            for row in partition:
                insert_query=''' INSERT INTO {0}
                VALUES({1},{2},{3})
                '''.format(table_name,row[0], row[1], row[2])

                cur.execute(insert_query)

        openconnection.commit()

    return 



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM {0} ;'.format(ratingstablename) )
        num_rows=cur.fetchone()[0]


        find_num_partitions ="""
                                SELECT COUNT(TABLE_NAME)
                                FROM INFORMATION_SCHEMA.tables 
                                WHERE TABLE_NAME LIKE '{0}%'
                                """.format(RROBIN_TABLE_PREFIX)

        cur.execute(find_num_partitions)
        num_partitions=cur.fetchone()[0]
        idx=num_rows%num_partitions

        query="""
                INSERT INTO {0} 
                VALUES({1},{2},{3})
                """.format(RROBIN_TABLE_PREFIX+str(idx), userid, itemid, rating )

        cur.execute(query)
        print('inserted value into '+RROBIN_TABLE_PREFIX+str(idx))

        insert_main_query='''INSERT INTO {0}
                        VALUES({1},{2},{3})
                    '''.format(ratingstablename,userid, itemid, rating)
        cur.execute(insert_main_query)

        print('inserted into '+ ratingstablename)

        openconnection.commit()


    return






def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
        
    with openconnection.cursor() as cur:

        find_num_partitions ="""
                                SELECT COUNT(TABLE_NAME)
                                FROM INFORMATION_SCHEMA.tables 
                                WHERE TABLE_NAME LIKE '{0}%'
                                """.format(RANGE_TABLE_PREFIX)

        cur.execute(find_num_partitions)
        num_partitions=cur.fetchone()[0]
        print(num_partitions)

        step=5.0/num_partitions

        idx=int(rating/step)
        if rating % step == 0 and idx != 0:
            idx = idx - 1
        table_name=RANGE_TABLE_PREFIX+ str(idx)

        insert_query="""
                INSERT INTO {0} 
                VALUES({1},{2},{3})
                """.format(table_name, userid, itemid, rating )


        cur.execute(insert_query)
        print('inserted into '+table_name)
        

        insert_main_query='''INSERT INTO {0}
                        VALUES({1},{2},{3})
                    '''.format(ratingstablename,userid, itemid, rating)
        cur.execute(insert_main_query)

        print('inserted into '+ratingstablename)

        openconnection.commit()


    return






    

























