#!/usr/bin/python3
import time
import datetime
import fdb
import settings


def get_query(query='query_1.sql', path='sql'):
    with open(path+'/'+query) as qf:
        return ''.join(qf.readlines())


db = fdb.connect(**settings.DB)

# SELECT = get_query('drivers.sql')
# c = db.cursor()
# c.execute(SELECT)
drivers = {}
# for r in c.fetchall():
#     term_acc, crew_id, _ = r
#     drivers[term_acc] = crew_id
# c.close()

SELECT = get_query('select_orders.sql')
ARGS = (datetime.datetime(2017, 8, 1, 6), datetime.datetime(2017, 9, 1, 8))
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
for account, statetime, tostate, _, crewid, orderid, dsumm, ord_crewid, drvid, border in c.fetchall():
    if account:
        # crew_id, shift_id, shift_time_begin, shift_time_end, disc_summ = r
        statetime = (statetime-datetime.timedelta(days=1)).date() if statetime.time().hour < 7 else statetime.date()
        crewid = int(crewid)
        # shift_id = int(shift_id)
        dsumm = float(dsumm) if dsumm else float_0
        # if crewid not in data:
        #     data[crew_id] = [shift_time_begin, shift_time_end, datetime.timedelta(0), 0, 0, 0]
        # print(account, statetime, tostate, crewid, orderid, dsumm, ord_crewid, drvid, border)
        if account not in drivers.keys():
            drivers[account] = drvid
        if drvid not in data.keys():
            data[drvid] = {}
        if statetime not in data[drvid]:
            data[drvid][statetime] = [0, 0, 0, 0, 0, 0.0]  # Всего, отказ, выполнено, отказ клиента, из них бордюр, сумма
        data[drvid][statetime][0] += 1
        if tostate == 4:  # Выполнен
            data[drvid][statetime][2] += 1
            if border == 1:
                data[drvid][statetime][4] += 1
            data[drvid][statetime][5] += dsumm
        if tostate == 9:  # Отказ
            data[drvid][statetime][1] += 1
        if tostate == 12:  # Отказ клиента
            data[drvid][statetime][3] += 1
        # print(crew_id, shift_id, shift_time_begin, shift_time_end, disc_summ)
        # data[crew_id][0] = min([data[crew_id][0], (shift_time_begin if shift_time_begin else date_inf)])
        # data[crew_id][1] = max([data[crew_id][1], (shift_time_end if shift_time_end else date_zero)])
        # data[crew_id][2] += (shift_time_end - shift_time_begin)
        # data[crew_id][3] += 1
        # data[crew_id][4] += (1 if disc_summ else 0)
        # data[crew_id][5] += disc_summ
c.close()
db.close()


for dr in sorted(drivers):  # Позывной
    if drivers[dr] in data.keys():
        print(dr)
        _total, _driver_cancel, _done, _cli_cancel, _border, _dsumm = 0, 0, 0, 0, 0, 0
        for d in sorted(data[drivers[dr]]):  # Дата
            total, driver_cancel, done, cli_cancel, border, dsumm = data[drivers[dr]][d]
            print('\t', d, total, driver_cancel, done, cli_cancel, border, dsumm)
            _total += total
            _driver_cancel += driver_cancel
            _done += done
            _cli_cancel += cli_cancel
            _border += border
            _dsumm += dsumm
        print('\t', len(data[drivers[dr]]), _total, _driver_cancel, _done, _cli_cancel, _border, _dsumm)

"""
with open('witch_hunter-%s-%s.csv' % (ARGS[0].date(), ARGS[1].date()), 'w') as rpt:
    rpt.writelines([';;;;Перемещения\n', 'Позывной;Начало периода;Конец периода;Всего времени;Всего;Заказы;Сумма\n'])
    for d in sorted(drivers):
        line_ = d
        if drivers[d] in data.keys():
            hours = data[drivers[d]][2].days*24 + data[drivers[d]][2].seconds // 3600
            minutes = (data[drivers[d]][2].seconds % 3600) // 60
            seconds = data[drivers[d]][2].seconds % 60
            data[drivers[d]][2] = '%s:%02d:%02d' % (hours, minutes, seconds)

            data[drivers[d]] = ['%s' % dr for dr in data[drivers[d]]]
            line_ += (';'+';'.join(data[drivers[d]]))
            line_ = line_.replace('.', ',')
        line_ += '\n'
        rpt.writelines([line_, ])
"""
