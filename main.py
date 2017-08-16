#!/usr/bin/python3
import time
import datetime
import fdb
import settings


def get_query(query='query_1.sql', path='sql'):
    with open(path+'/'+query) as qf:
        return ''.join(qf.readlines())


SELECT = get_query()
ARGS = (datetime.datetime(2017, 8, 1, 6, 0), datetime.datetime(2017, 9, 1, 8))
# print(SELECT % ARGS)
# db = fdb.connect(database='d:\\tme_db.fdb',
#                  host='127.0.0.1',
#                  user='sysdba',
#                  password='admin', charset='win1251')
db = fdb.connect(**settings.DB)
c = db.cursor()
c.execute(SELECT % ARGS)
c.fetchall()
db.close()
