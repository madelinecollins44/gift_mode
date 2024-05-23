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

create or replace temporary table rec_mod as (
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
        , split((select value from unnest(beacon.properties.key_value) where key = "listing_ids"), ",")as lisitngs_clean
    , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-2
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
	v._date >= current_date-2
group by all
);

---work in progress
with get_referrers as (
select 
  referrer
  , visit_id
  , sequence_number 
  , listing_id
  , url
from 
  etsy-data-warehouse-prod.weblog.events a
where 
  event_type in ('view_listing') 
  and (referrer like ('%gift-mode/occasion/%')
      or (referrer like ('%gift-mode/persona/%'))) -- add in search here 
)
, referrers as (
select
  referrer
  , INITCAP(REPLACE(REGEXP_SUBSTR(referrer, 'gift-mode/occasion/([^/?]+)'),"-"," ")) AS page_name
  , REGEXP_SUBSTR(referrer, 'gift-mode/([^/?]+)') AS page_type
  , visit_id
  , sequence_number 
  , listing_id
  , REGEXP_SUBSTR(url, 'gift_idea_id=([^&]+)') AS gift_idea_id
from 
  get_referrers
where 
  referrer like ('%gift-mode/occasion/%')
union all 
select
  referrer
  , INITCAP(REPLACE(REGEXP_SUBSTR(referrer, 'gift-mode/persona/([^/?]+)'),"-"," ")) AS page_name
  , REGEXP_SUBSTR(referrer, 'gift-mode/([^/?]+)') AS page_type
  , visit_id
  , sequence_number 
  , listing_id
  , REGEXP_SUBSTR(url, 'gift_idea_id=([^&]+)') AS gift_idea_id
from 
  get_referrers
where 
  referrer like ('%gift-mode/persona/%')
)
select 
  a.visit_id
  , b.page_name
  , b.page_type
  , regexp_substr(beacon.loc, "gift_idea_id=([^*&?%]+)") as gift_idea_id	-- exists on web only
  , purchased_after_view
from 
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  referrers b
    on a.visit_id=b.visit_id
    and a.sequence_number=b.sequence_number
    and a.listing_id=cast(b.listing_id as int64)
inner join 
  `etsy-visit-pipe-prod`.canonical.visit_id_beacons c 
    on a.visit_id=c.visit_id
    and a.sequence_number=c.sequence_number
    -- and a.listing_id=c.listing_id 
where
  	beacon.event_name = 'view_listing'
    and date(c._partitiontime)>= current_date-2
    and a._date >= current_date-2

