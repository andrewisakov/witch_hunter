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
drivers = {term_acc: crew_id for term_acc, crew_id, _ in c.fetchall()}
c.close()
# input(drivers['01024'])

SELECT = get_query('select_orders.sql')
ARGS = settings.DATES
c = db.cursor()
c.execute(SELECT, ARGS)
float_0 = float(0)
data = {}
date_zero = datetime.date(1, 1, 1)
date_inf = datetime.date(9999, 12, 31)
# print(date_zero)
for account, statetime, tostate, _, crewid, orderid, dsumm, ord_crewid, drvid, border in c.fetchall():
    if account:
        statetime = (statetime-datetime.timedelta(days=1)).date() if statetime.time().hour < 7 else statetime.date()
        crewid = int(crewid)
        ord_crewid = int(ord_crewid) if ord_crewid else 0
        dsumm = float(dsumm) if dsumm else float_0
        if crewid not in data.keys():
            data[crewid] = {}
        if statetime not in data[crewid]:
            data[crewid][statetime] = [datetime.timedelta(0), 0, 0, 0, 0, 0, 0.0]
            # время на линии, Всего, отказ, выполнено, отказ клиента, из них бордюр, сумма
        data[crewid][statetime][1] += 1
        if (tostate == 4) and (crewid == ord_crewid):  # Выполнен
            data[crewid][statetime][3] += 1
            if border == 1:
                data[crewid][statetime][5] += 1
            data[crewid][statetime][6] += dsumm
        if tostate == 9:  # Отказ
            data[crewid][statetime][2] += 1
        if tostate == 12:  # Отказ клиента
            data[crewid][statetime][4] += 1
        # if account == '01024':
        #     print(data[drvid])
c.close()
c = db.cursor()
SELECT = get_query('select_driver_smens.sql')
ARGS *= 2
c.execute(SELECT, ARGS)
# print(SELECT, ARGS)
for crewid, begin_time, end_time, term_acc in c.fetchall():
    if crewid:
        crewid = int(crewid)
        if crewid not in data.keys():
            data[crewid] = {}
        if begin_time.time() < datetime.time(7) <= end_time.time():
            dates = ((begin_time - datetime.timedelta(days=1)).date(), end_time.date(),)
            durations = {dates[0]: end_time-datetime.datetime(dates[1].year, dates[1].month, dates[1].day, hour=7),
                         dates[1]: datetime.datetime(dates[1].year, dates[1].month, dates[1].day, 7)-begin_time,}
        else:
            dates = (begin_time.date(),)
            durations = {dates[0]: end_time - begin_time,}
        for d in durations:
            date_zero = d if date_zero < d else date_zero
            date_inf = d if date_inf > d else date_inf
            if d not in data[crewid]:
                data[crewid][d] = [datetime.timedelta(0), 0, 0, 0, 0, 0, 0.0]
            data[crewid][d][0] += durations[d]
        # if drivers['01024'] == crewid:
        #     print(data[crewid])
c.close()
db.close()

# date_zero = date_zero.date()
# date_inf = date_inf.date()

# print(date_zero, date_inf)


for dr in sorted(drivers):  # Позывной
    if drivers[dr] in data.keys():
        # print(dr)
        _total, _driver_cancel, _done, _cli_cancel, _border, _dsumm, _duration = 0, 0, 0, 0, 0, 0, datetime.timedelta(0)
        for d in sorted(data[drivers[dr]]):  # Дата
            duration, total, driver_cancel, done, cli_cancel, border, dsumm = data[drivers[dr]][d]
            _total += total
            _driver_cancel += driver_cancel
            _done += done
            _cli_cancel += cli_cancel
            _border += border
            _dsumm += dsumm
            _duration += duration
        #     print('\t', d, total, driver_cancel, done, cli_cancel, border, dsumm, duration)
        # print('\t', len(data[drivers[dr]]), _total, _driver_cancel, _done, _cli_cancel, _border, _dsumm, _duration)


with open('witch_hunter-%s-%s.csv' % (ARGS[0].date(), ARGS[1].date()), 'w') as rpt:
    rpt.writelines([';;;;;;;;;'+';;;;;;;;'.join(['%s' % (ARGS[0]+datetime.timedelta(days=d)).date() for d in range((ARGS[1]-ARGS[0]).days)])+'\n'])
    rpt.writelines(['Позывной;'+'Время;Предложено;Отказ;Выполнено;Отказ клиента;Бордюр;Сумма;Ср.стоимость;'*((ARGS[1]-ARGS[0]).days+1) +'\n'])
    for dr in sorted(drivers):
        line_ = dr
        if drivers[dr] in data.keys():
            totals = [datetime.timedelta(0), 0, 0, 0, 0, 0, 0.0]
            for d in data[drivers[dr]]:
                for r in range(len(totals)):
                    totals[r] += data[drivers[dr]][d][r]
            hours = totals[0].days*24 + data[drivers[dr]][d][0].seconds // 3600
            minutes = (totals[0].seconds % 3600) // 60
            seconds = totals[0].seconds % 60
            totals[0] = '%d:%02d:%02d' % (hours, minutes, seconds)
            totals.append('%.2f' % (totals[6]/totals[3] if totals[3] > 0 else 0))
            totals[6] = '%.2f' % totals[6]
            totals = ['%s' % t for t in totals]
            line_ += ';' + ';'.join(totals)
            last_date = ARGS[0].date()
            for d in sorted(data[drivers[dr]]):
                if (d - last_date).days > 1:
                    line_ += (';'*((d - last_date).days-1)*8)
                    last_date = d

                # print(data[drivers[dr]][d])
                hours = data[drivers[dr]][d][0].days*24 + data[drivers[dr]][d][0].seconds // 3600
                minutes = (data[drivers[dr]][d][0].seconds % 3600) // 60
                seconds = data[drivers[dr]][d][0].seconds % 60
                data[drivers[dr]][d][0] = '%d:%02d:%02d' % (hours, minutes, seconds)
                data[drivers[dr]][d].append('%.2f' % (data[drivers[dr]][d][6]/data[drivers[dr]][d][3] if data[drivers[dr]][d][3] > 0 else 0))
                data[drivers[dr]][d][6] = '%.2f' % data[drivers[dr]][d][6]
                data[drivers[dr]][d] = ['%s' % dr for dr in data[drivers[dr]][d]]
                line_ += ';'+';'.join(data[drivers[dr]][d])
                line_ = line_.replace('.', ',')

        line_ += '\n'
        rpt.writelines([line_, ])
