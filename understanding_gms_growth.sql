------------------------------------------------------------------------
GIFT TITLE 
------------------------------------------------------------------------

--data on views + conversion rates
with get_views as (
select
  listing_id
  , count(visit_id) as views
  , count(case when purchased_in_visit = 1 then visit_id end) as purchased_in_visit
from
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  --  (c._date between date('2024-01-01') and date('2024-04-09'))
   (_date between date('2023-01-01') and date('2023-04-09'))
group by 1 
)
select 
case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , sum(c.views) as views 
  , sum(c.purchased_in_visit) 
  , sum(c.purchased_in_visit)  / sum(c.views) as conversion_rate
from 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
left join 
  get_views c 
    on b.listing_id=c.listing_id
group by 1


--data on inventory 
select 
case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
  , avg(a.price_usd)/100 as avg_price --this table  as listing price in cents
  , count(distinct top_category) as unique_categories
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
where 
  -- (a.date between '2024-01-01' and '2024-04-09')
  (a.date between date('2023-01-01') and date('2023-04-09'))
group by 1

	
-- number of listings that changed to have gift in title 
with titles as (
select 
v.listing_id
, case when regexp_contains(v.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
, case when regexp_contains(l.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title_now
from
  `etsy-data-warehouse-prod.schlep_views.transactions_vw` v
left join
  `etsy-data-warehouse-prod.etsy_shard.listings` l 
    on v.listing_id = l.listing_id
where date(timestamp(v.creation_tsz)) >= current_date-730
)

select 
count(distinct listing_id) as listings
, count(distinct case when gift_title = 1 then listing_id end) as gift_title_transaction
, count(distinct case when gift_title_now=1 then listing_id end) as gift_title_now
, count(distinct case when gift_title = 0 and gift_title_now=1 then listing_id end) as gift_title_change
from 
  titles 

--find gift in title inflation yoy
  select 
  '2019' as year
  , case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
where 
  (a.date between '2019-01-01' and '2019-04-09')
group by 1,2
union all 
select 
'2020' as year  
  , case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
where 
  (a.date between '2020-01-01' and '2020-04-09')
group by 1,2
union all 
select 
'2021' as year
  , case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
where 
  (a.date between '2021-01-01' and '2021-04-09')
group by 1,2
union all 
select 
'2022' as year
  , case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
where 
  (a.date between '2022-01-01' and '2022-04-09')
group by 1,2
union all 
select 
'2023' as year
, case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
where 
  (a.date between '2023-01-01' and '2023-04-09')
group by 1,2
union all 
select 
'2024' as year
  , case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
where 
  (a.date between '2024-01-01' and '2024-04-09')
group by 1,2
----------------------------------------------------
GIFT SEARCHING BEHAVIOR 
----------------------------------------------------
--find queries driving growth, then look at growth in queries by search_source 
with all_queries as (
SELECT
 query
  -- , case 
  --   when search_source like ('hp_%') then 'homepage'
  --   when search_source like ('catnav%') then 'catnav'
  --   when search_source like ('s2_qi%') then 'query_ingresses'
  --   else split(search_source, '-')[safe_offset(0)]
  -- end as search_source
  , rank() over (order by count(distinct visit_id) desc) as query_rank
	, count(distinct case when _date between "2024-01-01" and "2024-04-09" then visit_id end) as visits2024
  , count(distinct case when _date between "2023-01-01" and "2023-04-09" then visit_id end) as visits2023
  , count(distinct case when _date between "2024-01-01" and "2024-04-09" then visit_id end)-count(distinct case when _date between "2023-01-01" and "2023-04-09" then visit_id end) as visits_diff
FROM 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm 
    USING (query)
WHERE 
  _date >= '2023-01-01'
  and is_gift > 0
group by 1
)
select * from all_queries 
where query_rank < 50 
order by 5 desc

	
--yoy gift queries 
SELECT
  '2024' as year
  , is_gift
	, count(distinct qs.visit_id) as query_visits
  ,	count(distinct tv.visit_id) as transaction_visits
from 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
join 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
left join 
    etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
      on qs.visit_id=tv.visit_id
where (_date between '2024-01-01' and '2024-04-09')
group by 1,2
UNION ALL 
SELECT
   '2023' as year
  , is_gift
	, count(distinct qs.visit_id) as query_visits
  ,	count(distinct tv.visit_id) as transaction_visits
from 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
join 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
left join 
    etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
      on qs.visit_id=tv.visit_id
where (_date between '2023-01-01' and '2023-04-09')
group by 1,2
UNION ALL 
select
  '2022' as year
  , is_gift
	, count(distinct qs.visit_id) as query_visits
  ,	count(distinct tv.visit_id) as transaction_visits
from 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
join 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
left join 
    etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
      on qs.visit_id=tv.visit_id
where (_date between '2022-01-01' and '2022-04-09')
group by 1,2
UNION ALL 
select
  '2021' as year
  , is_gift
	, count(distinct qs.visit_id) as query_visits
  ,	count(distinct tv.visit_id) as transaction_visits
from 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
join 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
left join 
    etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
      on qs.visit_id=tv.visit_id
where (_date between '2021-01-01' and '2021-04-09')
group by 1,2
UNION ALL 
select
  '2020' as year
  , is_gift
	, count(distinct qs.visit_id) as query_visits
  ,	count(distinct tv.visit_id) as transaction_visits
from 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
join 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
left join 
    etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
      on qs.visit_id=tv.visit_id
where (_date between '2020-01-01' and '2020-04-09')
group by 1,2
UNION ALL 
select
  '2019' as year
  , is_gift
	, count(distinct qs.visit_id) as query_visits
  ,	count(distinct tv.visit_id) as transaction_visits
from 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
join 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
left join 
    etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
      on qs.visit_id=tv.visit_id
where (_date between '2019-01-01' and '2019-04-09')
group by 1,2

--visits w searches + transactions
select
is_gift
, count(distinct qs.visit_id) as query_visits
, count(distinct tv.visit_id) as transactions_visits
from 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
join  
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm 
    USING (query)
  left join 
    etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
    on qs.visit_id=tv.visit_id
-- where _date between "2024-01-01" and "2024-04-09"
where _date between "2023-01-01" and "2023-04-09"
group by 1 order by 2 desc



--top gift queries by distinct visit count
with all_queries as (
SELECT
 query
  , rank() over (order by count(distinct visit_id) desc) as query_rank
	, count(distinct case when _date between "2024-01-01" and "2024-04-09" then visit_id end) as visits2024
  , count(distinct case when _date between "2023-01-01" and "2023-04-09" then visit_id end) as visits2023
  , count(distinct case when _date between "2024-01-01" and "2024-04-09" then visit_id end)-count(distinct case when _date between "2023-01-01" and "2023-04-09" then visit_id end) as visits_diff
FROM 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm 
    USING (query)
WHERE 
  _date >= '2023-01-01'
  and is_gift > 0
group by 1
)
select * from all_queries 
where query_rank < 50 
order by 5 desc

--queries by search source
select 
   case 
    when search_source like ('hp_%') then 'homepage'
    when search_source like ('catnav%') then 'catnav'
    when search_source like ('s2_qi%') then 'query_ingresses'
    else split(search_source, '-')[safe_offset(0)]
  end as search_source
, count(distinct visit_id)
from etsy-data-warehouse-prod.search.events
where _date between "2024-01-01" and "2024-04-09"
group by 1


----------------------------------------------------
WHAT IS THE IMPACT OF LESS TIAG GMS?
----------------------------------------------------
--tiag orders yoy
select 
  '2019' as year
  , is_gift
  , count(distinct transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions
where 
  (date between '2019-01-01' and '2019-04-09')
group by 1,2
union all 
select 
'2020' as year  
  , is_gift
  , count(distinct transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions
where 
  (date between '2020-01-01' and '2020-04-09')
group by 1,2
union all 
select 
'2021' as year
  , is_gift
  , count(distinct transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions
where 
  (date between '2021-01-01' and '2021-04-09')
group by 1,2
union all 
select 
'2022' as year
  , is_gift
  , count(distinct transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions
where 
  (date between '2022-01-01' and '2022-04-09')
group by 1,2
union all 
select 
'2023' as year
  , is_gift
  , count(distinct transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions
where 
  (date between '2023-01-01' and '2023-04-09')
group by 1,2
union all 
select 
'2024' as year
  , is_gift
  , count(distinct transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions
where 
  (date between '2024-01-01' and '2024-04-09')
group by 1,2
