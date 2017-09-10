select d.term_account as account, oh.statetime, oh.tostate, os.name, oh.crewid,
oh.orderid, o.discountedsumm as dsumm, o.crewid as ord_crewid,
cr.driverid as drvid, o.fromborder as brd
from orders_h oh
left join orders o on (o.id=oh.orderid)
join order_states os on (os.id=oh.tostate)
join crews cr on (cr.id=oh.crewid)
join drivers d on (d.id=cr.driverid)
where oh.statetime between ? and ?
and oh.tostate in (4,9,12)
/* and cr.driverid=7663 */
order by d.term_account, oh.statetime
