select cr.driverid, df.begin_time, df.end_time
from drvsmens_fact df
join crews cr on (cr.id=df.crewid)
where (df.begin_time between ? and ?)
and (df.end_time between ? and ?)
