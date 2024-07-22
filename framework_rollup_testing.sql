
---------------------------------------------------------------
--find visit_id and check against weblog.events, impressions
---------------------------------------------------------------
--impressions
ith get_recmods_events as (
  select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
    , beacon.primary_event as primary_event
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = 'module_placement') as module_placement
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
	where 
    date(_partitiontime) >= current_date-5
    and --events
      (beacon.event_name like ('%gm_%') or beacon.event_name like ('%gift_mode%') --catpures most gm content + core
	      or (beacon.event_name = 'recommendations_module_delivered' -- rec mods from other outside gift mode 
            and (select value from unnest(beacon.properties.key_value) where key = 'module_placement') in     ('lp_suggested_personas_related','homescreen_gift_mode_personas') --related personas module on listing page, web AND app home popular personas module delivered, boe
            or (select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('market_gift_personas%')))--related/ popular persona module on market page, web
)
select 
	b._date  
	, a.platform 
  , a.browser_platform 
	, a.region  
  , a.is_admin_visit as admin 
  , a.top_channel 
  , visit_id
  , count(visit_id) as impressions
  , max(case when event_name like ('%gift_mode%') and primary_event=true then 1 else 0 end) as core_visits
  , count(case when event_name like ('%gift_mode%') and primary_event=true then visit_id end) as core_impressions
from 
  etsy-data-warehouse-prod.weblog.visits a
inner join 
  get_recmods_events b 
    using (_date, visit_id)
where 
  a._date >= current_date-2
group by all
---8C14J5SIBHQSvIuXFkFEu5gCzWpH.1720629452511.1: 1 core visit, 1 core impression, 8 gm impressions
---PX5W6YuPQEKhhunLQbDpXA.1720585837344.1: 372 core visit
--UayHm-slODTgbxWpR4S3QQc2znjI.1720724737747.1: 255 core visits

--test 
select 
  _date
  , visit_id
  , count(case when event_type like ('%gift_mode%') and page_view =1 then sequence_number end) as core_gm_views
from etsy-data-warehouse-prod.weblog.events 
where visit_id in ('UayHm-slODTgbxWpR4S3QQc2znjI.1720724737747.1','PX5W6YuPQEKhhunLQbDpXA.1720585837344.1','8C14J5SIBHQSvIuXFkFEu5gCzWpH.1720629452511.1')
group by all 


__________________________________________________________________________________________
--make sure ref tags make sense-- core vs non core (most will be core)
__________________________________________________________________________________________
with listing_views as (
select 
	_date 
	, visit_id 
	, sequence_number 
	, safe_cast(listing_id as int64) as listing_id
	, regexp_substr(e.referrer, "ref=([^*&?%|]+)") as boe_ref 
	, ref_tag
from 
	`etsy-data-warehouse-prod`.weblog.events e 
where 
	_date >= current_date-1
	and event_type = "view_listing"
  and ((ref_tag like ('gm_%') or ref_tag like ('listing_suggested_persona%')) -- nowhere a user can see a GM listing from outside a core page from heather
      or referrer like ('boe_gift_mode%')) 
  -- and (ref_tag like ('gm_gift_idea_listings%') -- persona listing view, web
  -- or ref_tag like ('gm_occasion_gift_idea_listings-%') -- occasion listing view, web
  -- or ref_tag like ('gm_deluxe_persona_card%') -- quiz listing view, web
  -- or ref_tag like ('listing_suggested_persona_listings_related%') -- listings at bottom of listing page, web, NOT CORE 
  -- or boe_referrer like ('boe_gift_mode_popular_gift_listings%') -- popular gift ideas listing view, web
  -- or boe_referrer like ('boe_gift_mode_gift_idea_listings%')-- persona listing view, boe
  -- or boe_referrer like ('gift_mode_occasion%')-- occasion listing view, boe
  -- or boe_referrer like ('boe_gift_mode_editors_picks_listings%'))-- occasion listing view, boe NEED TO CONFIRM
)
, agg as (
select
	b._date  
  , a.listing_id
  , a.visit_id
  , coalesce(count(*),0) as n_listing_views
  , max(case when a.ref_tag like ('gm%') then 1 else 0 end) as core_listing
from 
  listing_views a
inner join 
  etsy-data-warehouse-prod.weblog.visits b
    using (_date, visit_id)
left join 
  `etsy-data-warehouse-prod`.analytics.listing_views c
    on a.listing_id=c.listing_id
    and a.visit_id=c.visit_id
    and a._date=c._date
    and a.sequence_number=a.sequence_number 
where 
  b._date >= current_date-5
group by all
)
select * from agg where core_listing =0 and core_listing=1
----no core 
--OQ6dNJLCEJqWBW9wQ_PlWH8UWtRF.1721580349328.1, 1330600135, listing_suggested_persona_listings_related-3
--3_nbaW1zxlj_Gqlt2yuKGIFLPBVL.1721574776318.1, 1409391892, listing_suggested_persona_listings_related-3


---------------------------------------------------------------
--find visit_id and check against weblog.events, listing views
---------------------------------------------------------------
select * from etsy-data-warehouse-prod.analytics.listing_views where visit_id in ('OQ6dNJLCEJqWBW9wQ_PlWH8UWtRF.1721580349328.1','3_nbaW1zxlj_Gqlt2yuKGIFLPBVL.1721574776318.1') and _date >= current_date-3
-- these are listing_suggested views

select * from etsy-data-warehouse-prod.analytics.listing_views 
where visit_id in ('n-XXQOaYHiAOGkTE_A-RX2OMbbYB.1721550018504.1') 
and _date >= current_date-3
and listing_id = 1143385396
--shoudl be 80
--originally from rollup: n-XXQOaYHiAOGkTE_A-RX2OMbbYB.1721550018504.1, 1143385396, 6400 listing views
----wrong, realized need to fix
