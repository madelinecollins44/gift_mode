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
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
        -- , split((select value from unnest(beacon.properties.key_value) where key = "listing_ids"), ",")as lisitngs_clean
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
   , listing_id
from 
  all_gift_idea_deliveries a
cross join 
   unnest(split(listing_ids, ',')) as listing_id
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
  , listing_id
from 
  all_gift_idea_deliveries a
cross join 
   unnest(split(listing_ids, ',')) as listing_id
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
	, b.page_type
  , b.page_name
  , count(distinct b.listing_id) as unique_listings
	, coalesce(count(case when module_placement_clean in ("boe_gift_mode_gift_idea_listings", "gift_mode_gift_idea_listings") then v.visit_id end),0) as shown_persona_page
	, coalesce(count(case when module_placement_clean in ("gift_mode_occasion_gift_idea_listings") then v.visit_id end),0) as shown_occasions_page
from
	`etsy-data-warehouse-prod`.weblog.recent_visits v
join
	deliveries b
    using(_date, visit_id)
where
	v._date >= current_date-2
group by all
);

---work in progress

create or replace temporary table listing_views as (
with get_referrers as ( -- i found the referrers this way so it was easier to grab the occasion + persona names 
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
from 
  get_referrers
where 
  referrer like ('%gift-mode/persona/%')
)
, get_gift_id as ( -- this grabs the gift_idea_id from the beacons table
select -- this is only for mweb+desktop
 	date(_partitiontime)
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
    , beacon.loc as loc
    , (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id
		, regexp_substr(beacon.loc, "gift_idea_id=([^*&?%]+)") as gift_idea_id-- grabs gift idea
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`a
inner join  
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  beacon.event_name in ('view_listing')
  and beacon.loc like ('%gift_idea_id%')
  and date(_partitiontime) >= current_date-2 
  and b._date >= current_date-2 
  and b.platform in ('mobile_web','desktop')
)
select 
  a.visit_id
  , b.page_name
  , b.page_type
  , a.listing_id
  , a.sequence_number 
  , a.purchased_after_view
  , c.loc
  , c.gift_idea_id
  , case 
      when b.page_type in ('occasion') then INITCAP(REPLACE(d.slug))
      when b.page_type in ('persona') then e.name
      else 'error' 
    end as gift_idea
from 
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  get_gift_id c --i grab the view_listing event from beacons and tie it to analytics listings views
    on a.visit_id=c.visit_id
    and a.sequence_number=c.sequence_number
    and a.listing_id=cast(c.listing_id as int64)
left join 
  referrers b -- i grabbed the view_listing event from the events table and matched the referrers to the listing views
    on a.visit_id=b.visit_id
    and a.sequence_number=b.sequence_number
    and a.listing_id=cast(b.listing_id as int64)
left join 
    etsy-data-warehouse-prod.etsy_aux.gift_mode_gift_idea_entity d
    on c.gift_idea_id=cast(d.gift_idea_id as string)
left join 
  etsy-data-warehouse-dev.knowledge_base.gift_mode_semaphore_gift_idea e
    on c.gift_idea_id=e.semaphore_guid
where
  a._date >= current_date-2
);

