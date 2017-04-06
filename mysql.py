import pymysql.cursors
import os
import json
import base64
import datetime
import random
import os
import sys

# Connect to the database





def query_db(sql_syntax,target,db):
    try:
        if target=='test':
            connection = pymysql.connect(host='1',
                                         user='r',
                                         password='z1',
                                         db=db,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)

        else:
            connection = pymysql.connect(host='17',
                                         user='r',
                                         password='q3',
                                         db=db,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            # Create a new record
            sql = sql_syntax

            cursor.execute(sql)
            connection.commit()
            #print(cursor.description)
            rows=[]
            for row in cursor:

                rows.append(row)
            return rows

        connection.commit()
    except Exception as e:
        print (e)
        rows="no such row"
    finally:
        return rows
        connection.close()
