select /*df.crewid,*/ cr.driverid, df.begin_time, df.end_time /*, ch.from_parking, ch.to_parking */
from drvsmens_fact df
join crews cr on (cr.id=df.crewid)
/*join crews_h ch on (ch.id=df.crewid and ch.statetime between df.begin_time and df.end_time)*/
where (df.begin_time between '%s' and '%s')
and (df.end_time between '%s' and '%s')
/*and cr.driverid=7663
order by df.begin_time*/
