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
