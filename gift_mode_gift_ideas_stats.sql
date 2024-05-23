--owner: madelinecollins@etsy.com
--owner_team: product-asf@etsy.com
--description: a rollup for measuring engagement with the gift mode discovery experience

-- BEGIN

-- declare last_date date;

-- drop table if exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`;

-- create table if not exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`  (
-- );

-- in case of day 1, backfill for 30 days
-- set last_date = (select max(_date) from `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`);
--  if last_date is null then set last_date = (select min(_date)-1 from `etsy-data-warehouse-prod.weblog.events`);
--  end if;

-- set last_date = current_date - 2;

-- create or replace temporary table rec_mod as (
with all_gift_idea_deliveries as (
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
		, (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id
    -- , (select value from unnest(beacon.properties.key_value) where key = "refTag") as refTag
    -- , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
    , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-5
	  and beacon.event_name = "recommendations_module_delivered"
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_%")
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%")
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_gift_idea_listings%"))
			------make sure to add in search here 
)
, deliveries as (
select 
  a._date 
  , 'occasion' as page_type
  , b.slug as page_name 
  , a.gift_idea_id
  , a.visit_id
   , a.module_placement_clean
from 
  all_gift_idea_deliveries a
left join 
	etsy-data-warehouse-prod.etsy_aux.gift_mode_occasion_entity b
    on a.occasion_id = cast(b.occasion_id as string)
where 
  module_placement_clean in ('gift_mode_occasion_gift_idea_listings') 
group by all
union all
select 
  _date 
  , 'persona' as page_type
  , b.name as page_name 
  , a.gift_idea_id
  , a.visit_id
  , a.module_placement_clean
from 
  all_gift_idea_deliveries a
left join 
	`etsy-data-warehouse-dev.knowledge_base.gift_mode_semaphore_persona` b
    on a.persona_id = b.semaphore_guid
where 
  module_placement_clean in ('gift_mode_gift_idea_listings','boe_gift_mode_gift_idea_listings')
group by all
)
select
	v._date
	, v.platform
	, v.region
	, v.top_channel
	, v.is_admin_visit as admin
  , b.gift_idea_id
  , c.name as gift_idea
	, b.page_type
  , b.page_name
	, coalesce(count(case when module_placement_clean in ("boe_gift_mode_gift_idea_listings", "gift_mode_gift_idea_listings") then v.visit_id end),0) as shown_persona_page
	, coalesce(count(case when module_placement_clean in ("gift_mode_occasion_gift_idea_listings") then v.visit_id end),0) as shown_occasions_page
from
	`etsy-data-warehouse-prod`.weblog.recent_visits v
join
	deliveries b
    using(_date, visit_id)
left join 
  etsy-data-warehouse-dev.knowledge_base.gift_mode_semaphore_gift_idea c
    on b.gift_idea_id=c.semaphore_guid
where
	v._date >= current_date-5
group by all
-- );
