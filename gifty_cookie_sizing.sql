-------------------------------------------------------------------------
OVERALL COVERAGE + BASE TABLES
-------------------------------------------------------------------------
select count(distinct visit_id), sum(total_gms) from etsy-data-warehouse-prod.weblog.visits where _date>= current_date-30
--visits: 1035700479
--total gms: 904336970.79 

create or replace table etsy-data-warehouse-dev.madelinecollins.web_visits_last_30_days as (
select platform, _date, visit_id, total_gms from etsy-data-warehouse-prod.weblog.visits where _date>= current_date-30 and platform in ('mobile_web', 'desktop')
);

-------------------------------------------------------------------------
GIFT CATEGORY PAGE COVERAGE
-------------------------------------------------------------------------
select 
platform
, count(distinct v.visit_id) as visits
, sum(v.total_gms) as gms
from 
  etsy-data-warehouse-dev.madelinecollins.web_visits_last_30_days v
inner join 
  etsy-data-warehouse-prod.weblog.events e
    using (visit_id)
where 
  e.event_type in ('category_page_hub')
group by 1

-------------------------------------------------------------------------
GIFT MARKET PAGE COVERAGE
-------------------------------------------------------------------------

select 
platform
, count(distinct v.visit_id) as visits
, sum(v.total_gms) as gms
from 
  etsy-data-warehouse-dev.madelinecollins.web_visits_last_30_days v
inner join 
  etsy-data-warehouse-prod.weblog.events e
    using (visit_id)
where 
  e.event_type in ('market') 
  and regexp_contains(e.url, "(?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト")
group by 1

-------------------------------------------------------------------------
GIFT MODE PAGE
-------------------------------------------------------------------------
select 
platform
, e.event_type
, count(distinct v.visit_id) as visits
, sum(v.total_gms) as gms
from 
  etsy-data-warehouse-dev.madelinecollins.web_visits_last_30_days v
inner join 
  etsy-data-warehouse-prod.weblog.events e
    using (visit_id)
where 
  e.event_type like ('gift_mode%')
  and e.page_view=1
group by 1,2
order by 1 desc

-------------------------------------------------------------------------
GIFTINESS SCORE
--------------------------------------------------------------------------
with visit_ids as (
select  
  visit_id
from 
  etsy-data-warehouse-prod.analytics.listing_views l
inner join
  (select listing_id, avg(score) as score from etsy-data-warehouse-prod.knowledge_base.listing_giftiness_v3 where score>= 0.51 group by 1) g 
    on l.listing_id=g.listing_id
where 
  l._date>= current_date-30
group by 1
)
select
a.platform
, count(distinct a.visit_id) as visits
, sum(a.total_gms) as gms
from
  etsy-data-warehouse-dev.madelinecollins.web_visits_last_30_days a
inner join 
  visit_ids b
    using (visit_id)
group by 1

-------------------------------------------------------------------------
LISTING LANDINGS FROM 'GIFT' UTM CAMPAIGNS
--------------------------------------------------------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.web_listing_landings_last_30_days as (
-- select
-- _date
--   , platform 
--   , visit_id
--   , total_gms
--   , referring_url
--   , case 
--       when landing_event in ('view_listing') then regexp_substr(landing_event_url, "listing\\/(\\d*)")
--       else '0' 
--     end as listing_id 
-- from 
--   etsy-data-warehouse-prod.weblog.visits v
-- where 
--   _date >=current_date-30
--   and landing_event in ('view_listing')
--   and regexp_contains(utm_campaign, "(?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") -- ads only have 'gift' in title 
-- );

--all gift ads
select 
platform
, count(distinct visit_id) as visits
, sum(total_gms) as gms
from etsy-data-warehouse-dev.madelinecollins.web_listing_landings_last_30_days 
group by 1

--gift ads from google
select 
platform
, count(distinct visit_id) as visits
, sum(total_gms) as gms
from etsy-data-warehouse-dev.madelinecollins.web_listing_landings_last_30_days 
where regexp_contains(referring_url, "google") 
group by 1

-------------------------------------------------------------------------
CROSSOVER COVERAGE
--------------------------------------------------------------------------
with gifty_visits as (
select  
  visit_id
from 
  etsy-data-warehouse-prod.analytics.listing_views l
inner join
  (select listing_id, avg(score) as score from etsy-data-warehouse-prod.knowledge_base.listing_giftiness_v3 where score>= 0.51 group by 1) g 
    on l.listing_id=g.listing_id
where 
  l._date>= current_date-30
group by 1
)
, category_pg as (
select
  visit_id
  from etsy-data-warehouse-prod.weblog.events where event_type in ('category_page_hub')
)
, market_pg as (
select
  visit_id
  from etsy-data-warehouse-prod.weblog.events 
  where
     event_type in ('market') 
     and regexp_contains(url, "(?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト")
)
, gift_mode_pg as (
select
  visit_id
  from etsy-data-warehouse-prod.weblog.events 
  where
     event_type like ('%gift_mode%') 
)
, agg as (
select 
  platform
  , a.visit_id as visit_id
  , a.total_gms
  , case when b.visit_id is not null then 1 else 0 end as gifty_view
  , case when c.visit_id is not null then 1 else 0 end as gift_mode_view
  , case when d.visit_id is not null then 1 else 0 end as market_view
  , case when e.visit_id is not null then 1 else 0 end as category_view
from 
  etsy-data-warehouse-dev.madelinecollins.web_visits_last_30_days a 
left join
  gifty_visits b 
    using(visit_id)
left join gift_mode_pg c
  on a.visit_id=c.visit_id  
left join market_pg d
  on a.visit_id=d.visit_id  
left join category_pg e
  on a.visit_id=e.visit_id  
)
select
  platform
  , count(distinct visit_id)
  , sum(total_gms)
from agg
where gifty_view=1 or gift_mode_view=1
group by 1

where gifty_view=1 or gift_mode_view=1
group by 1

