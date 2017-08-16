select d.term_account, c.id, d.deleted
from drivers d
join crews c on (c.driverid=d.id)
where d.term_account like '0%' and d.deleted = 0
order by d.term_account
