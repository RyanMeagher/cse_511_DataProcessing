
import psycopg2	

def create_output_table(table1,table2, table2_join_column, openconnection):
	cur=openconnection.cursor()
    query=''' SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{0}';
	'''.format(InputTable2)

    cur.execute(query)
    schema2=cur.fetchall()
    schema2=[item[0] for item in schema2 if item[0]!=Table1JoinColumn]

    out_col=[out_table2+'.'+item for item in schema2]
    out_col_str=','.join(out_col)


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


conn=getOpenConnection()

cur=conn.cursor()
