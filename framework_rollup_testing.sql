
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

---------------------------------------------------------------
--find visit_id and check against weblog.events, listing views
---------------------------------------------------------------
