------------------------------------------------------------------------
GIFT TITLE 
------------------------------------------------------------------------
---year over year change in listings with 'gift' in title
select 
case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
  , avg(a.price_usd)/100 as avg_price --this table  as listing price in cents
  , count(distinct top_category) as unique_categories
  , count(c.visit_id) as views 
  , count(case when c.purchased_in_visit = 1 then c.visit_id end) as purchased_in_visit 
  , count(case when c.purchased_in_visit = 1 then c.visit_id end) / count(c.visit_id) as conversion_rate
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
left join 
  etsy-data-warehouse-prod.analytics.listing_views c 
    on a.listing_id=c.listing_id
where 
  (a.date between '2024-01-01' and '2024-04-09')
  -- (a.date between date('2023-01-01') and date('2023-04-09'))
  and (c._date between date('2024-01-01') and date('2024-04-09'))
  -- and (c._date between date('2023-01-01') and date('2023-04-09'))
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
--yoy gift queries 
  SELECT
  '2024' as year
	, count(distinct visit_id)
from `etsy-data-warehouse-prod.search.query_sessions_new` qs
join `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
where (_date between '2024-01-01' and '2024-04-09')
and is_gift > 0
UNION ALL 
SELECT
  '2023' as year
	, count(distinct visit_id)
from `etsy-data-warehouse-prod.search.query_sessions_new` qs
join `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
where (_date between '2023-01-01' and '2023-04-09')
and is_gift > 0
UNION ALL 
SELECT
  '2022' as year
	, count(distinct visit_id)
from `etsy-data-warehouse-prod.search.query_sessions_new` qs
join `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
where (_date between '2022-01-01' and '2022-04-09')
and is_gift > 0
UNION ALL 
SELECT
  '2021' as year
	, count(distinct visit_id)from `etsy-data-warehouse-prod.search.query_sessions_new` qs
join `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
where (_date between '2021-01-01' and '2021-04-09')
and is_gift > 0
UNION ALL 
SELECT
  '2020' as year
	, count(distinct visit_id)from `etsy-data-warehouse-prod.search.query_sessions_new` qs
join `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
where (_date between '2020-01-01' and '2020-04-09')
and is_gift > 0
UNION ALL 
SELECT
  '2019' as year
	, count(distinct visit_id)from `etsy-data-warehouse-prod.search.query_sessions_new` qs
join `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
where (_date between '2019-01-01' and '2019-04-09')
and is_gift > 0
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
