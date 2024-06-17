--------------------------------------------------------------------------------
--create base dataset of events + in between events
--------------------------------------------------------------------------------
-- create base dataset of events 

create or replace table `etsy-data-warehouse-dev.madelinecollins.mothers_day_events` 
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

