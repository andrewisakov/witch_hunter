select /*cr.driverid*/df.crewid, df.begin_time, df.end_time, dr.term_account
from drvsmens_fact df
join crews cr on (cr.id=df.crewid)
join drivers dr on (dr.id=cr.driverid)
where ((df.begin_time between ? and ?)
or (df.end_time between ? and ?))
and df.end_time not is null
