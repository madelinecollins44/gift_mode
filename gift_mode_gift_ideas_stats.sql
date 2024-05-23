--owner: madelinecollins@etsy.com
--owner_team: product-asf@etsy.com
--description: a rollup for measuring engagement with the gift mode discovery experience

BEGIN

declare last_date date;

-- drop table if exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`;

create table if not exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`  (
	_date DATE 
	, platform STRING
	, region STRING 
	, persona_id STRING 
	, persona_name STRING 
    , admin INT64
    , top_channel STRING
	, shown_quiz_results INT64
	, shown_popular_personas INT64
	, shown_relevant_personas INT64
	, shown_feeling_stuck INT64
	, shown_more_quiz_results INT64
	, all_pageviews INT64
	, unique_visits INT64
	, ref_popular_personas INT64
	, ref_quiz_results INT64
	, ref_more_quiz_results INT64
	, ref_relevant_personas INT64
	, ref_feeling_stuck_personas INT64
	, ref_onsite_banners INT64
    , count_view_listing INT64
	, count_exits INT64
	, total_listing_views INT64
	, total_persona_page_listing_views INT64
	, unique_listings_viewed INT64
	, unique_transactions INT64
	, total_purchased_listings INT64
	, attr_gms NUMERIC
);

-- in case of day 1, backfill for 30 days
set last_date = (select max(_date) from `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`);
 if last_date is null then set last_date = (select min(_date)-1 from `etsy-data-warehouse-prod.weblog.events`);
 end if;

-- set last_date = current_date - 2;

-- gather all information about all gift mode recommendation module deliveries 
-- 	including combining all persona_id fields into a single column and cleaning up module_placement for easier joins

create or replace temporary table rec_mod as (
with all_recs as (
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
		, (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id
    , (select value from unnest(beacon.properties.key_value) where key = "refTag") as refTag
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids

	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-1
	and beacon.event_name = "recommendations_module_delivered"
	and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_%") 
      or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%"))
			------make sure to add in search here 
)
--, multi_recs as (
select -- these modules use target for its persona property
	_date
	, visit_id
	, sequence_number
	, event_name
	, module_placement
	, gift_idea_id
	, refTag
	, (regexp_substr(b,"([a-z0-9-%+]+)")) as listing_id
from 
	all_recs
cross join unnest(split(listing_ids, ",")) b
)
select
	v._date
	, v.platform
	, v.region
	, v.top_channel
	, v.is_admin_visit as admin
	, b.persona_id
	, c.name
	, coalesce(count(case when module_placement_clean in ("boe_gift_mode_deluxe_persona_card", "gift_mode_deluxe_persona_card") then v.visit_id end),0) as shown_quiz_results
	, coalesce(count(case when module_placement_clean in ("gift_mode_popular_personas","boe_gift_mode_popular_personas") then v.visit_id end),0) as shown_popular_personas
	, coalesce(count(case when module_placement_clean in ("gift_mode_relevant_persona_ideas", "boe_gift_mode_related_personas") then v.visit_id end),0) as shown_relevant_personas
	, coalesce(count(case when module_placement_clean in ("gift_mode_feeling_stuck_web", "boe_gift_mode_categorized_personas") then v.visit_id end),0) as shown_feeling_stuck
	, coalesce(count(case when module_placement_clean in ("gift_mode_more_quiz_results") then v.visit_id end),0) as shown_more_quiz_results
from
	`etsy-data-warehouse-prod`.weblog.recent_visits v
join
	multi_recs b
using(_date, visit_id)
left join
	`etsy-data-warehouse-dev.knowledge_base.gift_mode_semaphore_persona` c
on
	b.persona_id = c.semaphore_guid
where
	v._date >= last_date
group by 1,2,3,4,5,6,7
);


create or replace temporary table persona_engagement as (
with all_persona_pageviews as (
select
	a._date 
	, a.visit_id 
	, a.event_type
	, a.sequence_number 
	, case when a.ref_tag like "gm-%" then ref_tag else split(a.ref_tag, "-")[safe_offset(0)] end as ref_tag
	, a.module_placement
	, a.persona_id 
	, a.next_page
from 
(
	select
		date(_partitiontime) as _date
		, visit_id
		, beacon.event_name as event_type
		, sequence_number
		, regexp_substr(beacon.loc, "ref=([^*&?%]+)") as ref_tag	-- exists on web only
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement  -- exists on BOE only 
		, (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id -- exists on web AND BOE
		, lead(beacon.event_name) over(partition by visit_id order by sequence_number) as next_page
	from
		`etsy-visit-pipe-prod`.canonical.visit_id_beacons
	where date(_partitiontime) >= last_date
	and (beacon.primary_event = true or beacon.event_name = "gift_mode_persona_tapped")
) a
where 
	event_type in ("gift_mode_persona", "gift_mode_persona_tapped")
), get_visit_data as (
select 
	a._date 
	, a.visit_id 
	, v.platform 
	, v.top_channel 
	, v.region 
	, v.is_admin_visit 
	, a.event_type 
	, a.sequence_number 
	, a.ref_tag 
	, a.module_placement
	, a.persona_id
	, b.name as persona_name
	, a.next_page
from 
	all_persona_pageviews a 
join 
	`etsy-data-warehouse-prod`.weblog.recent_visits v 
using(_date, visit_id)
left join 
	`etsy-data-warehouse-dev.knowledge_base.gift_mode_semaphore_persona` b 
on 
	a.persona_id = b.semaphore_guid
where 
	v._date >= last_date
), clean_up as (
select 
	distinct _date 
	, visit_id 
	, platform 
	, top_channel 
	, region 
	, is_admin_visit as admin
	, ref_tag as clean_referrer 
	, persona_id 
	, persona_name
	, next_page
	, sequence_number
from 
	get_visit_data 
where 
	platform in ("desktop", "mobile_web")
	and event_type = "gift_mode_persona"
union all 
select 
	_date 
	, visit_id 
	, platform 
	, top_channel 
	, region 
	, is_admin_visit as admin
	, module_placement as clean_referrer 
	, persona_id 
	, persona_name
	, next_page
	, sequence_number
from 
	get_visit_data 
where 
	platform = "boe"
	and event_type = "gift_mode_persona_tapped"
)
-- , aggregate as (
select 
	b._date
	, b.platform
	, b.region
	, b.persona_id
	, b.persona_name
	, b.admin
	, b.top_channel
	, coalesce(count(b.visit_id),0) as all_pageviews
	, coalesce(count(distinct b.visit_id),0) as unique_visits
--------for refs, web = ref_tags and boe= module_placements? 
	, (coalesce(count(case when clean_referrer in ("gm_popular_personas","boe_gift_mode_popular_personas") then b.visit_id end),0)) as ref_popular_personas 
	, coalesce(count(case when clean_referrer in ("gm_deluxe_persona_card","boe_gift_mode_deluxe_persona_card","boe_gift_mode_quiz_results_listings") then b.visit_id end),0) as ref_quiz_results
	, coalesce(count(case when clean_referrer in ("gm_more_quiz_results") then b.visit_id end),0) as ref_more_quiz_results
	, coalesce(count(case when clean_referrer in ("gm_relevant_persona_ideas","boe_gift_mode_related_personas") then b.visit_id end),0) as ref_relevant_personas 
	, coalesce(count(case when clean_referrer in ("boe_gift_mode_categorized_personas","gm_feeling_stuck") then b.visit_id end),0) as ref_feeling_stuck_personas
	, coalesce(count(case when clean_referrer like "gm-%" then b.visit_id end),0) as ref_onsite_banners 
	, coalesce(count(case when next_page in ("view_listing") then b.visit_id end),0) as count_view_listing
	, coalesce(count(case when next_page is null then b.visit_id end),0) as count_exits
from 
	clean_up b
group by 1,2,3,4,5,6,7
)
;


-- grab more detailed data for gift mode recommendations that contain listings 

create or replace temporary table gm_personas as (
with persona_events as (
select
	date(a._partitiontime) as _date
	, a.visit_id
	, beacon.event_name
	, a.sequence_number
	, beacon.loc
	, beacon.ref
	, (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
	, (select value from unnest(beacon.properties.key_value) where key = "persona_ids") as persona_ids
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
	date(a._partitiontime) >= last_date
	and beacon.event_name in ("gift_mode_persona", "gift_mode_results", "gift_mode_quiz_results_delivered")
)
, tmp as (
select
a.*
, b
from
	persona_events a
	,unnest(array(
	select as struct persona_id, persona_id_offset
	from unnest((split(regexp_replace(persona_ids,"\\[|\\]", ""), ","))) as persona_id with offset as persona_id_offset
	)) b
)
-- , together as (
select
	_date
	, visit_id
	, event_name
	, sequence_number
	, loc
	, ref
	, coalesce(persona_id, regexp_substr(b.persona_id,"([a-z0-9-%+]+)")) as persona_id
	, safe_cast(b.persona_id_offset as int64) + 1 as persona_id_offset
from
	tmp
union all
select
	_date
	, visit_id
	, event_name
	, sequence_number
	, loc
	, ref
	, persona_id
	, NULL as persona_id_offset
from
	persona_events
where
	event_name = "gift_mode_persona"
)
;

-- -- get table of all clicked listings from personas and quiz results

create or replace temporary table listing_clicks as ( 
with views as (
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
	_date >= last_date
	and event_type = "view_listing"
)
select
	_date
	, visit_id
	, platform
	, listing_id
	, sequence_number
	, purchased_after_view
	, case when lv.platform = "boe" then b.boe_ref else lv.ref_tag end as ref_tag
	, case 
		when lv.ref_tag like "gm_deluxe_persona_card%" then split(lv.ref_tag, "-")[safe_offset(1)] 
		when b.boe_ref like "boe_gift_mode_quiz_results_listings%" then split(b.boe_ref, "-")[safe_offset(1)] 
		end as persona_index
	, referring_page_event
	, referring_page_event_sequence_number
	, visit_id || "_" || listing_id as listing_visit_id
from
	`etsy-data-warehouse-prod`.analytics.listing_views lv
left join 
	views b 
using(_date, visit_id, listing_id, sequence_number)
where
	_date >= last_date
and 
	referring_page_event in ("gift_mode_persona", "gift_mode_quiz_results", "gift_mode_results")
)
;

-- get GMS data at the visit_id / listing_id level for join

create or replace temporary table listing_gms as (
select
	tv.date as _date
	, tv.visit_id
	, tv.platform_app as platform
	, tv.transaction_id
	, t.listing_id
	, tg.trans_gms_net
from
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv
join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans tg
using(transaction_id)
join
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t
on
	tv.transaction_id = t.transaction_id
where
	tv.date >= last_date
)
;

-- -- connect listing views and purchases to the relevant personas

create or replace temporary table clicks_purchases as (
--identifying clicks from a persona page is simple since the referring page event is only associated with
--a single persona_id
with clicks as (
select 
	a._date 
	, v.platform 
	, v.region 
	, v.is_admin_visit 
	, v.top_channel 
	, a.persona_id 
	, a.visit_id 
	, b.listing_id 
	, b.ref_tag
	, coalesce(count(*),0) as n_listing_views
	, coalesce(max(purchased_after_view),0) as purchased_after_view
from
	gm_personas a
join
	`etsy-data-warehouse-prod`.weblog.recent_visits v
on
	a.visit_id = v.visit_id
left join
	listing_clicks b
on
	a._date = b._date
	and a.visit_id = b.visit_id
	and a.sequence_number = b.referring_page_event_sequence_number
where
	event_name = "gift_mode_persona"
	and v._date >= last_date
group by 1,2,3,4,5,6,7,8,9
union all
--identifying clicks directly from a persona card in quiz results means we need to match
--the position of the persona_id in a given set of results with the positional indicator on
--the ref tag
select
	a._date
	, v.platform
	, v.region
	, v.is_admin_visit
	, v.top_channel
	, a.persona_id
	, a.visit_id
	, b.listing_id
	, b.ref_tag
	, coalesce(count(b.listing_id),0) as n_listing_views
	, coalesce(max(b.purchased_after_view),0) as purchased_after_view
from
	gm_personas a
join
	`etsy-data-warehouse-prod`.weblog.recent_visits v
on
	a.visit_id = v.visit_id
left join
	listing_clicks b
on
a._date = b._date
and a.visit_id = b.visit_id
and a.sequence_number = b.referring_page_event_sequence_number
and a.persona_id_offset = safe_cast(b.persona_index as int64)
and (b.ref_tag like "gm_deluxe_persona_card%" or ref_tag like "boe_gift_mode_quiz_results_listings%")
where
	a.event_name in ("gift_mode_results", "gift_mode_quiz_results_delivered")
	and v._date >= last_date
group by 1,2,3,4,5,6,7,8,9
)
select
	a._date
	, a.platform
	, a.region
	, a.is_admin_visit
	, a.top_channel
	, a.persona_id
	, sum(a.n_listing_views) as total_listing_views
	, coalesce(count(case when (a.ref_tag like ('boe_gift_mode_gift_idea_listings%') or a.ref_tag like ('gm_gift_idea_listings%')) then a.visit_id end),0) as total_persona_page_listing_views
	, count(distinct a.listing_id) as unique_listings_viewed
	, count(distinct transaction_id) as unique_transactions
	, sum(a.purchased_after_view) as total_purchased_listings
	, coalesce(sum(b.trans_gms_net),0) as attr_gms
from
	clicks a
left join
	listing_gms b
on
	a._date = b._date
	and a.platform = b.platform
	and a.visit_id = b.visit_id
	and a.listing_id = b.listing_id
	and a.purchased_after_view > 0
group by 1,2,3,4,5,6
)
;

delete from `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats` where _date >= last_date;

-- bring it all together & insert

-- insert into `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats` (
insert into `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats` (
select 
	a._date 
	, a.platform 
	, case when a.region in ("US", "GB", "DE", "FR", "CA", "AU") then a.region else "RoW" end as region
	, a.persona_id 
	, a.name as persona_name
    , a.admin
    , a.top_channel 
	, coalesce(a.shown_quiz_results,0)
	, coalesce(a.shown_popular_personas,0)
	, coalesce(a.shown_relevant_personas,0)
	, coalesce(a.shown_feeling_stuck,0)
	, coalesce(a.shown_more_quiz_results,0)
	, coalesce(b.all_pageviews,0) 
	, coalesce(b.unique_visits,0)
	, coalesce(b.ref_popular_personas,0)
	, coalesce(b.ref_quiz_results,0)
	, coalesce(b.ref_more_quiz_results,0)
	, coalesce(b.ref_relevant_personas,0)
	, coalesce(b.ref_feeling_stuck_personas,0)
	, coalesce(b.ref_onsite_banners,0)
  , coalesce(b.count_view_listing,0)
	, coalesce(b.count_exits,0)
	, coalesce(c.total_listing_views,0)
	, coalesce(c.total_persona_page_listing_views,0)
	, coalesce(c.unique_listings_viewed,0)
	, coalesce(c.unique_transactions,0)
	, coalesce(c.total_purchased_listings,0)
	, coalesce(c.attr_gms,0)
from 
	rec_mod a 
left join 
	persona_engagement b 
on 
	a._date = b._date 
	and a.platform = b.platform 
	and a.region = b.region 
	and a.persona_id = b.persona_id
	and a.admin = b.admin
	and a.top_channel = b.top_channel
left join 
	clicks_purchases c 
on 
	a._date = c._date 
	and a.platform = c.platform 
	and a.region = c.region 
	and a.persona_id = c.persona_id
	and a.admin = c.is_admin_visit
	and a.top_channel = c.top_channel
)
;

END
