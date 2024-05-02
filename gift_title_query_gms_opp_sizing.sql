---------------------------------------------
VISITS + GMS IN LAST 30 DAYS
--------------------------------------------
--overall visits 
select count(distinct visit_id), sum(total_gms) from etsy-data-warehouse-prod.weblog.visits where _date>= current_date-30

--visits with query 
with raw as 
  (select distinct visit_id from etsy-data-warehouse-prod.search.query_sessions_new where _date >= current_date-30)
select 
count(distinct visit_id) as unique_visits_with_query
, sum(b.total_gms) as search_gms
from raw a
inner join etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where b._date>= current_date-30  

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
where 
b._date>= current_date-30 
--and query not like ('%gift%')
group by all
order by 3 desc

	
------------------------------------
QUERIES BY GIFTINESS SCORE
------------------------------------
with raw as (
select
  visit_id
  , avg(overall_giftiness)
from 
  etsy-data-warehouse-prod.knowledge_base.query_giftiness a
inner join 
  etsy-data-warehouse-prod.search.query_sessions_new b 
    on a.query=b.query
    and a._date=b._date -- gets avg giftiness score for queries from visit date
where a._date >= current_date-30 and b._date >= current_date-30
group by all
having avg(overall_giftiness) >= 0.51
)
select 
count(distinct visit_id) as unique_visits
, sum(total_gms) as total_gms
from 
  raw a
inner join 
  etsy-data-warehouse-prod.weblog.visits b
    using (visit_id)
where b._date >= current_date-30

--examples of queries by giftiness score
select
  a.query
  , count(visit_id) as sessions
  , avg(overall_giftiness) as avg_score
from 
  etsy-data-warehouse-prod.knowledge_base.query_giftiness a
inner join 
  etsy-data-warehouse-prod.search.query_sessions_new b 
    on a.query=b.query
    and a._date=b._date -- gets avg giftiness score for queries from visit date
where a._date >= current_date-5 and b._date >= current_date-5
group by all
having 
  avg(overall_giftiness) >= 0.41 
  and avg(overall_giftiness) <= 0.51 
  and count(visit_id) >= 10000
order by 2 desc

------------------------------------
GIFT QUERY
------------------------------------
with get_visits as (
SELECT
	visit_id
  , max(case when is_gift > 0 then 1 else 0 end) as is_gift
  , max(case when is_holiday > 0 then 1 else 0 end) as is_holiday
  , max(case when is_occasion > 0 then 1 else 0 end) as is_occasion
  , max(case when is_gift > 0 or is_holiday >0 or (is_occasion > 0 and is_gift > 0) then 1 else 0 end) as is_gift_holiday_giftoccasion
  , max(case when is_gift > 0 or is_holiday >0 or is_occasion > 0 or regexp_contains(qm.query, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end) as is_gift_holiday_occasion_regex
  , max(case when regexp_contains(qm.query, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end) as regex
  , max(case when qm.query like ('%card%') and qm.query not like ('%business%') and qm.query not like ('%tarot%')and qm.query not like ('%playing%')and qm.query not like ('%playing%')and qm.query not like ('%deck%')  then 1 else 0 end) as greeting_card
  , max(case when regexp_contains(qm.query, "(\?i)\\bpersonalize|\\bunique|\\bhandmade|\\bcustom") then 1 else 0 end) as gift_attributes
  , max(case when regexp_contains(qm.query, "(\?i)\\bearring|\\bnecklace|\\bbracelet|\\baccessory|\\bjewelry|\\bcup|\\bmug|\\bcandle") then 1 else 0 end) as gift_items
  , max(case when regexp_contains(qm.query, "(\?i)\\bcarepackage|\\bcare package") then 1 else 0 end) as carepackage
  , max(case when regexp_contains(qm.query, "(\?i)\\bgiftbox|\\bgift box") then 1 else 0 end) as giftbox
  , max(case when regexp_contains(qm.query, "(\?i)\\bpresent") then 1 else 0 end) as present
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE 
	_date >= current_date - 30
group by all
)
select
count(distinct case when is_gift =1 then a.visit_id end) as visits_is_gift
, count(distinct case when is_holiday =1 then a.visit_id end) as visits_is_holiday
, count(distinct case when is_occasion =1 then a.visit_id end) as visits_is_occasion
, count(distinct case when is_gift_holiday_giftoccasion =1 then a.visit_id end) as visits_is_gift_holiday_giftoccasion
, count(distinct case when is_gift_holiday_occasion_regex =1 then a.visit_id end) as visits_is_gift_holiday_occasion_regex
, count(distinct case when regex =1 then a.visit_id end) as visits_regex
, count(distinct case when greeting_card =1 then a.visit_id end) as visits_greeting_card
, count(distinct case when gift_attributes =1 then a.visit_id end) as visits_gift_attributes
, count(distinct case when gift_items =1 then a.visit_id end) as visits_gift_items
, count(distinct case when carepackage =1 then a.visit_id end) as visits_carepackage
, count(distinct case when giftbox =1 then a.visit_id end) as visits_giftbox
, count(distinct case when present =1 then a.visit_id end) as visits_present
, sum(case when is_gift =1 then b.total_gms end) as gms_is_gift
, sum(case when is_holiday =1 then b.total_gms end) as gms_is_holiday
, sum(case when is_occasion =1 then b.total_gms end) as gms_is_occasion
, sum(case when is_gift_holiday_giftoccasion =1 then b.total_gms end) as gms_is_gift_holiday_giftoccasion
, sum(case when is_gift_holiday_occasion_regex =1 then b.total_gms end) as gms_is_gift_holiday_occasion_regex
, sum(case when regex =1 then b.total_gms end) as gms_regex
, sum(case when greeting_card =1 then b.total_gms end) as gms_greeting_card
, sum(case when gift_attributes =1 then b.total_gms end) as gms_gift_attributes
, sum(case when gift_items =1 then b.total_gms end) as gms_gift_items
, sum(case when carepackage =1 then b.total_gms end) as gms_carepackage
, sum(case when giftbox =1 then b.total_gms end) as gms_giftbox
, sum(case when present =1 then b.total_gms end) as gms_present
from 
  get_visits a
inner join 
  etsy-data-warehouse-prod.weblog.visits b 
    using (visit_id)
where b._date>= current_date-30  


------------------------------------------------------------------------
LISTING RESULTS ON FIRST PAGE OF SEARCH RESULTS
------------------------------------------------------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.gifty_score_first_search_pg as (
-- select 
--   a.query
--   , a.listing_id
--   , avg(b.score) as score
--   , a.visit_id
-- from 
--   etsy-data-warehouse-prod.rollups.organic_impressions a
-- inner join 
--   etsy-data-warehouse-dev.knowledge_base.listing_giftiness_v3 b
--     -- on a._date=date(timestamp_seconds(b.run_date))
--     on a.listing_id=b.listing_id
-- where 
--   a.placement in ('search', 'async_listings_search', 'browselistings', 'search_results') 
--   and _date>= current_date-30
--   and page_number in ('1')
--   and query not like ('%gift%')
-- group by all
-- );

with gifty_score as (
select 
  query
  , listing_id
  , score
from 
  etsy-data-warehouse-dev.madelinecollins.gifty_score_first_search_pg
group by all
), query_count as (
select distinct 
  query
  , visit_id
from 
  etsy-data-warehouse-dev.madelinecollins.gifty_score_first_search_pg 
group by all
)
select 
  b.query  
  , count(a.query) as sessions
  , avg(b.score) as score
from query_count a
inner join gifty_score b 
    using (query)
  group by all

--visit coverage 
with gifty_score as (
select 
  query
  , listing_id
  , score
from 
  etsy-data-warehouse-dev.madelinecollins.gifty_score_first_search_pg
group by all
), query_count as (
select distinct 
  query
  , visit_id
from 
  etsy-data-warehouse-dev.madelinecollins.gifty_score_first_search_pg 
group by all
)
, visit_level as (
select 
  a.visit_id
  , case 
      when avg(b.score) >= 0 and avg(b.score) < 0.2 then '< 0.2'
      when avg(b.score) <= 0.3 then '<= 0.3'
      when avg(b.score) <= 0.4 then '<= 0.4'
      when avg(b.score) <= 0.5 then '<= 0.5'
      when avg(b.score) <= 0.6 then '<= 0.6'    
      when avg(b.score) <= 0.7 then '<= 0.7'
      when avg(b.score) <= 0.8 then '<= 0.8'    
      when avg(b.score) <= 0.9 then '<= 0.9'
      when avg(b.score) <= 1.0 then '<= 1'
    end as score
from query_count a
inner join gifty_score b 
      using (query)
group by all
having count(a.query) >= 10000
)
select
a.score 
  , count(distinct a.visit_id) as unique_visits
  , sum(b.total_gms) as total_gms
from 
  visit_level a
inner join 
  etsy-data-warehouse-prod.weblog.visits b 
  using(visit_id)
where b._date>= current_date-5
group by all
