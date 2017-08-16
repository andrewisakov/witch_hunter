#!/usr/bin/python3
import time
import datetime
import fdb
import settings


def get_query(query='query_1.sql', path='sql'):
    with open(path+'/'+query) as qf:
        return ''.join(qf.readlines())


db = fdb.connect(**settings.DB)

SELECT = get_query('drivers.sql')
c = db.cursor()
c.execute(SELECT)
drivers = {}
for r in c.fetchall():
    term_acc, crew_id, _ = r
    drivers[term_acc] = crew_id
c.close()

SELECT = get_query()
ARGS = (datetime.datetime(2017, 8, 1, 6, 0), datetime.datetime(2017, 9, 1, 8))
# print(SELECT % ARGS)
# db = fdb.connect(database='d:\\tme_db.fdb',
#                  host='127.0.0.1',
#                  user='sysdba',
#                  password='admin', charset='win1251')
c = db.cursor()
c.execute(SELECT % ARGS)
float_0 = float(0)
data = {}
date_zero = datetime.datetime(1, 1, 1, 0, 0, 0)
date_inf = datetime.datetime(9999, 12, 31, 23, 59, 59)
# print(date_zero)
for r in c.fetchall():
    crew_id, shift_id, shift_time_begin, shift_time_end, disc_summ = r
    crew_id = int(crew_id)
    shift_id = int(shift_id)
    disc_summ = float(disc_summ) if disc_summ else float_0
    if crew_id not in data:
        data[crew_id] = [shift_time_begin, shift_time_end, datetime.timedelta(0), 0, 0, 0]
    # print(crew_id, shift_id, shift_time_begin, shift_time_end, disc_summ)
    data[crew_id][0] = min([data[crew_id][0], (shift_time_begin if shift_time_begin else date_inf)])
    data[crew_id][1] = max([data[crew_id][1], (shift_time_end if shift_time_end else date_zero)])
    data[crew_id][2] += (shift_time_end - shift_time_begin)
    data[crew_id][3] += 1
    data[crew_id][4] += (1 if disc_summ else 0)
    data[crew_id][5] += disc_summ
c.close()
db.close()

with open('witch_hunter-%s-%s.csv' % (ARGS[0].date(), ARGS[1].date()), 'w') as rpt:
    rpt.writelines(['Позывной;Начало периода;Конец периода;Перемещения\n', ';;;Всего;Заказы;Сумма\n'])
    for d in sorted(drivers):
        line_ = d
        if drivers[d] in data.keys():
            hours = data[drivers[d]][2].days*24 + data[drivers[d]][2].seconds // 3600
            minutes = (data[drivers[d]][2].seconds % 3600) // 60
            seconds = data[drivers[d]][2].seconds % 60
            data[drivers[d]][2] = '%s:%s:%s' % (hours, minutes, seconds)
            # data[drivers[d]][2].days*24 + data[drivers[d]][2].seconds // 3600, data[drivers[d]][2].seconds % 3600, 
            data[drivers[d]] = ['%s' % dr for dr in data[drivers[d]]]
            line_ += (';'+';'.join(data[drivers[d]]))
            line_ = line_.replace('.', ',')
        line_ += '\n'
        rpt.writelines([line_, ])
