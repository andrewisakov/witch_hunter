select ch.crewid, dsf.id as shift_id, dsf.begin_time, dsf.end_time, o.discountedsumm
from crews_h ch
left join orders o on ((o.crewid=ch.crewid) and
ch.statetime between o.starttime and o.finishtime)
join drvsmens_fact dsf on ((dsf.crewid=ch.crewid) and
ch.statetime between dsf.begin_time and dsf.end_time)
where (ch.from_parking <> ch.to_parking) and
ch.statetime between '%s' and '%s'
group by ch.crewid, shift_id, dsf.begin_time, dsf.end_time
order by ch.crewid, shift_id, dsf.begin_time, dsf.end_time
