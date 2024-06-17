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
