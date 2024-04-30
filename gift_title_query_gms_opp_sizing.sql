---------------------------------------------
TOP QUERIES IN TIAG ORDERS
--------------------------------------------
with tiag_orders as (
select 
  a.is_gift
  , b.visit_id
  , sum(c.trans_gms_net) as trans_gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions a
inner join 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits b
  using (transaction_id)
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans c
    on a.transaction_id=c.transaction_id
where 
  a.date >= current_date-30
  and a.is_gift=1
group by all
)
select 
query 
, count(distinct a.visit_id) as unique_visits
, count(a.visit_id) as total_searches
from 
  tiag_orders a
inner join 
  etsy-data-warehouse-prod.search.query_sessions_new b
    using (visit_id)
where b._date>= current_date-30
group by all
order by 3 desc

	
------------------
QUERIES BY GIFTINESS SCORE
----------------
select
  count(distinct visit_id) as unique_visits
  , count(visit_id) as searches
  , count(distinct query) as unique_queries 
from 
  etsy-data-warehouse-prod.knowledge_base.query_giftiness a
inner join 
  etsy-data-warehouse-prod.search.query_sessions_new b using (query)
where 
  a._date >= current_date-30 
  and b._date >= current_date-30 
group by all 
having avg(overall_giftiness) >= 0.61
order by 2 desc
	
------------------
GIFT QUERY
----------------
with get_visits as (
SELECT
	visit_id
  , is_gift
  , is_holiday
  , is_occasion
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE 
	_date >= current_date - 60
  and ((is_gift > 0 or is_holiday > 0 or is_occasion > 0) 
  or regexp_contains(qm.query, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト"))
)
select
is_gift
, is_holiday
, is_occasion
, count(distinct visit_id)
etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans
--37425603

SELECT
	count(distinct visit_id)
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE 
	_date >= current_date - 60
  and is_gift > 0
--13773862

SELECT
	count(distinct visit_id)
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE 
	_date >= current_date - 60
  and ((is_gift > 0 or is_holiday > 0)
  or (is_gift > 0 and is_occasion > 0) -- this is bc occasion can be wonky 
  or regexp_contains(qm.query, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト"))
--21491454


------------------
GIFT TITLE
----------------
