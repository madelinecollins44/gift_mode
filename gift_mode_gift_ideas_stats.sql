--owner: madelinecollins@etsy.com
--owner_team: product-asf@etsy.com
--description: a rollup for measuring engagement with the gift mode discovery experience

BEGIN

declare last_date date;

drop table if exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`;

create table if not exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`  (
	_date DATE
	, platform STRING
	, region STRING
	, top_channel STRING
	, admin int64
  , gift_idea_id STRING
  , page_type STRING
  , page_name STRING
  -- , unique_listings int64
	, shown_persona_page  int64
	, shown_occasions_page int64
	, shown_search_page int64
  , clicks int64
  , total_listing_views int64
  , unique_listings_viewed int64
  , unique_transactions int64
  , total_purchased_listings int64
  , attr_gms NUMERIC
);

-- in case of day 1, backfill for 30 days
-- set last_date = (select max(_date) from `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`);
--  if last_date is null then set last_date = (select min(_date)-1 from `etsy-data-warehouse-prod.weblog.events`);
--  end if;

set last_date = current_date - 2;

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
    , (select value from unnest(beacon.properties.key_value) where key = "gift_idea_ids") as gift_idea_ids -- this is for boe gift mode search, has gift_idea_id and persona_id 
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
    , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-2
	  and beacon.event_name = "recommendations_module_delivered"
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_%") -- mweb/ desktop occasions
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%") -- mweb/ desktop personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_gift_idea_listings%") -- boe personas
	      or (select value from unnest(beacon.properties.key_value) where key = "module_placement") in ("boe_gift_mode_search_gift_ideas") -- boe search-- gift_idea_deliveries 
      	or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_search_listings%") -- boe search-- actual listing deliveries
))
, cleaned_boe_search as (
select 
  _date 
  , 'persona' as page_type -- keeping this as persona bc hats whay gift_idea_ids array says 
  , a.page_id
  , a.gift_idea_id_search as gift_idea_id
  , a.visit_id
  , a.module_placement_clean
	, b.listing_id -- this comes from boe_gift_mode_search_listings
from 
  (select 
    *
    , split(split(gift_idea_ids, '"persona_id":"')[safe_offset(1)], '"')[safe_offset(0)] AS page_id
    , split(split(gift_idea_ids, '"gift_idea_id":"')[safe_offset(1)], '"')[safe_offset(0)] AS gift_idea_id_search
    from 
      all_gift_idea_deliveries 
    where 
      module_placement_clean in ('boe_gift_mode_search_gift_ideas')) a -- this is where i pull out persona_id + gift_idea_id for the search modules loaded
left join 
  (select * from all_gift_idea_deliveries where module_placement_clean in ('boe_gift_mode_search_listings')) b
    on a.visit_id=b.visit_id
    and a.gift_idea_id_search=b.gift_idea_id
)
, deliveries as (
select  ---------------------- this is for mweb/ desktop occasions
  a._date 
  , 'occasion' as page_type
  , a.occasion_id as page_id 
  , a.gift_idea_id
  , a.visit_id
   , a.module_placement_clean
  --  , listing_id
from 
  all_gift_idea_deliveries a
-- cross join 
--    unnest(split(listing_ids, ',')) as listing_id
where 
  module_placement_clean in ('gift_mode_occasion_gift_idea_listings') 
group by all
union all
select  ---------------------- this is for all personas
  _date 
  , 'persona' as page_type
  , a.persona_id as page_id
  , a.gift_idea_id
  , a.visit_id
  , a.module_placement_clean
  -- , listing_id
from 
  all_gift_idea_deliveries a
-- cross join 
--    unnest(split(listing_ids, ',')) as listing_id
where 
  module_placement_clean in ('gift_mode_gift_idea_listings','boe_gift_mode_gift_idea_listings') --currently, boe does not have gift_idea_id here
group by all
union all
select  ---------------------- this is for all boe search 
   _date 
  , 'persona' as page_type
  , a.page_id
  , a.gift_idea_id
  , a.visit_id
  , a.module_placement_clean
  -- , listing_id
from cleaned_boe_search 
-- cross join 
--    unnest(split(listing_ids, ',')) as listing_id
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
  , b.page_id
  -- , count(distinct b.listing_id) as unique_listings
	, coalesce(count(case when module_placement_clean in ("boe_gift_mode_gift_idea_listings", "gift_mode_gift_idea_listings") then v.visit_id end),0) as shown_persona_page
	, coalesce(count(case when module_placement_clean in ("gift_mode_occasion_gift_idea_listings") then v.visit_id end),0) as shown_occasions_page
	, coalesce(count(case when module_placement_clean in ("boe_gift_mode_search_gift_ideas") then v.visit_id end),0) as shown_search_page
from
	`etsy-data-warehouse-prod`.weblog.recent_visits v
join
	deliveries b
    using(_date, visit_id)
where
	v._date >= current_date-2
group by all
);

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

--get all primary events for places gift ideas deliver (use this as referring info)
create or replace temporary table clicks as (
with get_primary_pages as ( -- all clicks to gift ideas should come from persona or occasion page (soon to add search) 
select
	date(_partitiontime) as _date
	, visit_id
	, sequence_number
	, beacon.event_name as event_name
  , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
  , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where date(_partitiontime) >= last_date
	and beacon.event_name in ('gift_mode_persona','gift_mode_occasions_page') -- will need to add search 
), referring_primary_page as (
select -- this gets the occasion_id from the referring page
	_date
	, visit_id
	, sequence_number
	, event_name
  , occasion_id as page_id
from get_primary_pages
where event_name in ('gift_mode_occasions_page')
union all 
select -- this gets the persona_id from the referring page
	_date
	, visit_id
	, sequence_number
	, event_name
  , persona_id as page_id
from get_primary_pages
where event_name in ('gift_mode_persona')
)
, get_refs_tags as (
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
  and date(_partitiontime) >= last_date
  and b._date >= last_date
  and b.platform in ('mobile_web','desktop')
------union all: for boe will use referrers 
)
, clicks as (
select 
  a._date
  , b.gift_idea_id
  , c.event_name 
  , c.page_id -- referring 
  , a.listing_id
  , a.visit_id
  , coalesce(count(a.listing_id),0) as n_listing_views
  , coalesce(max(a.purchased_after_view),0) as purchased_after_view
from 
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  get_refs_tags b
    on a.listing_id=cast(b.listing_id as int64)
    and a.sequence_number=b.sequence_number
    and a.visit_id=b.visit_id
left join  
  referring_primary_page c
    on c.event_name=a.referring_page_event
    and c.sequence_number=a.referring_page_event_sequence_number
    and a.visit_id=c.visit_id
where
  a._date >= last_date
group by all 
)
select
 a._date
  , a.gift_idea_id
  , a.event_name
  , a.referring_page_event
  , a.page_id -- referring 
  , count(a.visit_id) as clicks
  , sum(a.n_listing_views) as total_listing_views
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
  and a.visit_id = b.visit_id
  and a.listing_id = b.listing_id
  and a.purchased_after_view > 0 -- this means there must have been a purchase 
group by all
);

insert into `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats` (
select 
	a._date
	, a.platform
	, a.region
	, a.top_channel
	, a.admin
  , a.gift_idea_id
  , a.page_type
  , case 
      when c.name is not null then c.name
      when d.slug is not null then d.slug
      else 'error'
    end as page_name
  -- , count(distinct b.listing_id) as unique_listings
	, coalesce(shown_persona_page) as shown_persona_page 
	, coalesce(shown_occasions_page) as shown_occasions_page 
	, coalesce(shown_search_page) as shown_search_page 
  , coalesce(b.clicks) as clicks
  , coalesce(b.total_listing_views) as total_listing_views
  , coalesce(b.unique_listings_viewed) as unique_listings_viewed
  , coalesce(b.unique_transactions) as unique_transactions
  , coalesce(b.total_purchased_listings) as total_purchased_listings
  , coalesce(b.attr_gms) as attr_gms
from 
	rec_mod a 
left join 
	clicks b 
    on a._date = b._date 
    and a.gift_idea_id = b.gift_idea_id
    and a.page_id = b.page_id
    and a.page_type = b.event_name
left join 
  `etsy-data-warehouse-dev.knowledge_base.gift_mode_semaphore_persona` c
    on a.page_id = c.semaphore_guid 
left join 
  etsy-data-warehouse-prod.etsy_aux.gift_mode_occasion_entity d
    on a.page_id = cast(d.occasion_id as string)
);

END




------------------------------------------------------------------------------------------------------------------------------------------------
web only-- need a lot of data from boe
------------------------------------------------------------------------------------------------------------------------------------------------
--owner: madelinecollins@etsy.com
--owner_team: product-asf@etsy.com
--description: a rollup for measuring engagement with the gift mode discovery experience

BEGIN

declare last_date date;

drop table if exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`;

create table if not exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`  (
	_date DATE
	, platform STRING
	, region STRING
	, top_channel STRING
	, admin int64
  , gift_idea_id STRING
  , gift_idea_name STRING
  , page_type STRING
  , referring_page_event STRING
  , page_name STRING
  , unique_listings_delivered int64
  , total_deliveries int64
	, shown_persona_page  int64
	, shown_occasions_page int64
  , clicks int64
  , total_listing_views int64
  , unique_listings_viewed int64
  , unique_transactions int64
  , total_purchased_listings int64
  , attr_gms NUMERIC
);

-- in case of day 1, backfill for 30 days
-- set last_date = (select max(_date) from `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`);
--  if last_date is null then set last_date = (select min(_date)-1 from `etsy-data-warehouse-prod.weblog.events`);
--  end if;

set last_date = current_date - 2;

-- create or replace temporary table gift_idea_names as (
-- select
-- from
--   etsy-data-warehouse-prod.etsy_aux.gift_mode_occasion_entity 
-- union all 
-- select
-- name as name
-- semaphore_guid as gift_idea_id
-- from etsy-data-warehouse-dev.knowledge_base.gift_mode_semaphore_gift_idea


-- )

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
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
    , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-2
	  and beacon.event_name = "recommendations_module_delivered"
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_%") -- mweb/ desktop occasions
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%") -- mweb/ desktop personas
))
, deliveries as (
select  ---------------------- this is for mweb/ desktop occasions
  a._date 
  , 'occasion' as page_type
  , a.occasion_id as page_id 
  , a.gift_idea_id
  , a.visit_id
   , a.module_placement_clean
   , listing_id
from 
  all_gift_idea_deliveries a
cross join 
   unnest(split(listing_ids, ',')) as listing_id
where 
  module_placement_clean in ('gift_mode_occasion_gift_idea_listings') 
group by all
union all
select  ---------------------- this is for all personas
  _date 
  , 'persona' as page_type
  , a.persona_id as page_id
  , a.gift_idea_id
  , a.visit_id
  , a.module_placement_clean
  , listing_id
from 
  all_gift_idea_deliveries a
cross join 
   unnest(split(listing_ids, ',')) as listing_id
where 
  module_placement_clean in ('gift_mode_gift_idea_listings','boe_gift_mode_gift_idea_listings') --currently, boe does not have gift_idea_id here
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
  , b.page_id
  , count(distinct b.listing_id) as unique_listings
  , count(visit_id) as total_deliveries
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

--get all primary events for places gift ideas deliver (use this as referring info)
create or replace temporary table clicks as (
with get_primary_pages as ( -- all clicks to gift ideas should come from persona or occasion page (soon to add search) 
select
	date(_partitiontime) as _date
	, visit_id
	, sequence_number
	, beacon.event_name as event_name
  , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
  , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where date(_partitiontime) >= last_date
	and beacon.event_name in ('gift_mode_persona','gift_mode_occasions_page') -- will need to add search 
), referring_primary_page as (
select -- this gets the occasion_id from the referring page
	_date
	, visit_id
	, sequence_number
	, event_name
  , occasion_id as page_id
from get_primary_pages
where event_name in ('gift_mode_occasions_page')
union all 
select -- this gets the persona_id from the referring page
	_date
	, visit_id
	, sequence_number
	, event_name
  , persona_id as page_id
from get_primary_pages
where event_name in ('gift_mode_persona')
)
, get_refs_tags as (
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
  and date(_partitiontime) >= last_date
  and b._date >= last_date
  and b.platform in ('mobile_web','desktop')
------union all: for boe will use referrers 
)
, clicks as (
select 
v._date
	, v.platform
	, v.region
	, v.top_channel
  , v.is_admin_visit as admin  
  , b.gift_idea_id
  , c.event_name 
  , c.page_id -- referring 
  , a.referring_page_event
  , a.listing_id
  , a.visit_id
  , coalesce(count(a.listing_id),0) as n_listing_views
  , coalesce(max(a.purchased_after_view),0) as purchased_after_view
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join -- only want listing views
  etsy-data-warehouse-prod.analytics.listing_views a
    using (visit_id)
inner join -- only want listings viewed from a loc with 'gift_idea_id' in title
  get_refs_tags b
    on a.listing_id=cast(b.listing_id as int64)
    and a.sequence_number=b.sequence_number
    and a.visit_id=b.visit_id
left join  
  referring_primary_page c -- left join here so can grab all referring events 
    on c.event_name=a.referring_page_event
    and c.sequence_number=a.referring_page_event_sequence_number
    and a.visit_id=c.visit_id
where
  a._date >= last_date
  and v._date >= last_date
group by all 
)
select
 	a._date
	, a.platform
	, a.region
	, a.top_channel
	, a.admin
  , a.gift_idea_id
  , a.page_id -- referring 
  , count(a.visit_id) as clicks
  , sum(a.n_listing_views) as total_listing_views
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
  and a.visit_id = b.visit_id
  and a.listing_id = b.listing_id
  and a.platform = b.platform
  and a.purchased_after_view > 0 -- this means there must have been a purchase 
group by all
);

insert into `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats` (
select 
	a._date
	, a.platform
	, a.region
	, a.top_channel
	, a.admin
  , a.gift_idea_id
  , e.slug as gift_idea_name
  , a.page_type
  , case 
      when c.name is not null then c.name
      when d.slug is not null then initcap(replace(d.slug, '-', ' ')) 
      else 'error'
    end as page_name
	, coalesce(unique_listings,0) as unique_listings_delivered 
  , coalesce(total_deliveries,0) as total_deliveries 
	, coalesce(shown_persona_page,0) as shown_persona_page 
	, coalesce(shown_occasions_page,0) as shown_occasions_page 
  , coalesce(b.clicks,0) as clicks
  , coalesce(b.total_listing_views,0) as total_listing_views
  , coalesce(b.unique_listings_viewed,0) as unique_listings_viewed
  , coalesce(b.unique_transactions,0) as unique_transactions
  , coalesce(b.total_purchased_listings,0) as total_purchased_listings
  , coalesce(b.attr_gms,0) as attr_gms
from 
	rec_mod a 
left join 
	clicks b 
    on a._date = b._date 
    and a.gift_idea_id = b.gift_idea_id
    and a.page_id = b.page_id
    and a.platform=b.platform
  	and a.region=b.region
	  and a.top_channel=b.top_channel
	  and a.admin=b.admin
left join 
  `etsy-data-warehouse-dev.knowledge_base.gift_mode_semaphore_persona` c
    on a.page_id = c.semaphore_guid 
left join 
  etsy-data-warehouse-prod.etsy_aux.gift_mode_occasion_entity d
    on a.page_id = cast(d.occasion_id as string)
left join
  etsy-data-warehouse-prod.etsy_aux.gift_mode_gift_idea_entity e
    on a.gift_idea_id=e.gift_idea_id
);

END
