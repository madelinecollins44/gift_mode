--------------------------------------------------------------------------------
--create base dataset of events + in between events
--------------------------------------------------------------------------------
-- create base dataset of events 
create or replace table `etsy-data-warehouse-dev.madelinecollins.occasion_page_events` 
	as (
with tmp as (
select 
	_date 
	, visit_id 
	, timestamp_millis(epoch_ms) as epoch_ms
	, event_type 
	, sequence_number
	, url 
	, ref_tag
	, lag(event_type) over(partition by visit_id order by sequence_number) as prior_page
	, lag(timestamp_millis(epoch_ms)) over(partition by visit_id order by sequence_number) as prior_epoch_ms
	, lag(ref_tag) over(partition by visit_id order by sequence_number) as prior_ref
	, lag(sequence_number) over(partition by visit_id order by sequence_number) as prior_seq
	, lead(event_type) over(partition by visit_id order by sequence_number) as next_page
	, lead(timestamp_millis(epoch_ms)) over(partition by visit_id order by sequence_number) as next_epoch_ms
	, lead(ref_tag) over(partition by visit_id order by sequence_number) as next_ref
	, lead(sequence_number) over(partition by visit_id order by sequence_number) as next_seq
from 
	`etsy-data-warehouse-prod.weblog.events`
where 
	_date >= current_date - 30
	and page_view = 1
)
select 
	* 
from 
	tmp
where
	event_type = "gift_mode_occasions_page"
)
;


--get inbetween events 
create or replace table `etsy-data-warehouse-dev.madelinecollins.occasion_page_between_events`
	as (
with events as (
select 
	date(_partitiontime) as _date 
	, a.visit_id 
	, beacon.event_name as event_type
	, beacon.primary_event as page_view
	, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
	, (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id
	, a.sequence_number
from 
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
join 
	`etsy-data-warehouse-dev.madelinecollins.occasion_page_events` 
using(visit_id)
where 
	date(_partitiontime) >= current_date - 30
)
select 
	a._date 
	, a.visit_id 
	, a.sequence_number as primary_event_sequence
	, e.event_type 
	, e.module_placement
	, e.gift_idea_id
	, e.sequence_number 
from 
	`etsy-data-warehouse-dev.madelinecollins.occasion_page_events` a
left join 
	events e
on 
	a.visit_id = e.visit_id 
	and e.page_view = false
	and e.sequence_number > a.sequence_number
	and (e.sequence_number < next_seq or next_seq is null)
)
;

--------------------------------------------------------------------------------
--pageviews + lv rate + cr on similar pages for comparisons
--------------------------------------------------------------------------------
---click rate/ lv rate of page types
with events as (
select
_date
, visit_id
, sequence_number
, event_type
-- , lead(event_type) over (partition by visit_id order by sequence_number) as next_page
from 
	etsy-data-warehouse-prod.weblog.events e
where _date >= current_date-30 
and (event_type in ('gift_mode_occasions_page', 'gift_mode_persona') 
or (event_type in ('category_page_hub') and url like ('%/c/gifts%')) --category page hub gifts
or (event_type in ('market') and regexp_contains(e.url, "(?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト")))--market page gifts
group by all 
)
, pageviews as (
select 
event_type 
, count(visit_id) as impressions
, count(distinct visit_id) as unique_visits  
from events
group by all
)
, listing_views as (
select
 a.referring_page_event --opted to find listing views this way bc market pages/ category pages dont have ref tag 
	, a.visit_id
	, a.listing_id
	, a.purchased_after_view
from 
	etsy-data-warehouse-prod.analytics.listing_views a
inner join 	
	(select _date, visit_id, sequence_number, event_type from events) b
		on a.visit_id=b.visit_id
		and a.referring_page_event=b.event_type
		and a.referring_page_event_sequence_number=b.sequence_number
where a._date >= current_date-30
), listing_views_agg as (
select
	 referring_page_event	
	, count(listing_id) as listing_views
	, sum(purchased_after_view) as purchases
from listing_views
group by all
)
select
	a.event_type
	, sum(a.impressions) as impressions
	, sum(b.listing_views) as listing_views
	, coalesce(b.purchases,0) as purchases
from pageviews a
left join listing_views_agg b
	on a.event_type=b.referring_page_event
group by all
	
--------------------------------------------------------------------------------
--look at 'module' clicks by listing ref tags 
--------------------------------------------------------------------------------
with listing_views as (
select 
  ref_tag, 
  visit_id, 
  listing_id, 
  purchased_after_view 
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  ref_tag like ('gm_occasion_gift_idea_listings%') 
  and _date >= current_Date-30 group by all
)
, listing_agg as (
select
 split(ref_tag,"-")[safe_offset(1)] as gift_idea_module_placement
 , count(listing_id) as listing_views
 , count(distinct listing_id) as listings_viewed
 , sum(purchased_after_view) as purchases 
from listing_views
group by all 
)
, deliveries as (
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(1)] as gift_idea_module_placement

	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-5
	  and beacon.event_name in ("recommendations_module_delivered",'recommendations_module_seen')
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_%") -- mweb/ desktop occasions
))
, deliveries_agg as (
select
  gift_idea_module_placement
  , count(case when event_name in ('recommendations_module_delivered') then module_placement end) as delivered
  , count(case when event_name in ('recommendations_module_seen') then module_placement end) as seen
  from deliveries
  group by all
)
select
a.delivered
, a.seen
, b.listing_views
, b.listings_viewed
, b.purchases 
from deliveries_agg a
left join listing_agg b using (gift_idea_module_placement)
	
--------------------------------------------------------------------------------
--how far are people scrolling
--------------------------------------------------------------------------------
with deliveries as (
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
		, beacon.page_guid
		, count(case when beacon.event_name in ('gift_mode_occasions_page') then sequence_number end) as gm_occasion_pageviews
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
    , cast(split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(1)] as int64) as gift_idea_module_placement
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-15
	  and ((beacon.event_name in ('recommendations_module_seen') and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_%")))
		or beacon.event_name in ('gift_mode_occasions_page'))
		-- and visit_id in ('76PF8gN_Oro1IaYzMZ7C4yixzy5v.1718634489107.1')
group by all
), agg as (
select
	_date
	, visit_id
	, sum(gm_occasion_pageviews) as gm_occasion_pageviews
	, count(gift_idea_module_placement) as total_modules_seen
	, max(gift_idea_module_placement) as highest_module_seen_in_visit
from deliveries
group by all
)
select
	_date
	, count(distinct visit_id) as unique_visits
	, sum(gm_occasion_pageviews) as total_pageviews
	, sum(total_modules_seen) as total_modules_seen
	, avg(highest_module_seen_in_visit) as avg_max_module_seen_in_visit
	, max(highest_module_seen_in_visit) as max_module_seen_in_visit
from agg
group by all 



 
