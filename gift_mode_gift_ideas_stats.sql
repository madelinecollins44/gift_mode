--owner: madelinecollins@etsy.com
--owner_team: product-asf@etsy.com
--description: a rollup for measuring engagement with the gift mode discovery experience

BEGIN

declare last_date date;

-- drop table if exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`;

create table if not exists `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`  (
	_date date
	, platform STRING
	, browser_platform STRING
	, region STRING
	, top_channel STRING
	, admin INT64
  , gift_idea_id STRING
  , gift_idea_title STRING
  , creator STRING
  , delivery_page STRING
  , delivery_title STRING
  , unique_listings_delivered INT64
  , unique_visits INT64
  , total_impressions INT64
  , total_listings_delivered INT64
  , visits_with_a_click int64
  , total_listing_views INT64
  , unique_listings_viewed INT64
  , unique_transactions INT64
  , total_purchased_listings INT64
  , attr_gms NUMERIC
);

-- in case of day 1, backfill for 30 days
-- set last_date= (select max(_date) from `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats`);
--  if last_date is null then set last_date= (select min(_date)-1 from `etsy-data-warehouse-prod.weblog.events`);
--  end if;

set last_date= current_date - 1;

--create table to pull all gift_ideas from both occasions and personas 
create or replace temporary table gift_idea_names as (
select
  initcap(replace(slug,'-',' ')) as name
  , cast(gift_idea_id as string) as gift_idea_id
  , b.auth_username as creator
from
  etsy-data-warehouse-prod.etsy_aux.gift_mode_gift_idea_entity a -- for everything from gems/ occasions 
left join 
  etsy-data-warehouse-prod.etsy_aux.staff b on a.staff_id=b.id
union all 
select
  name as name
  , semaphore_guid as gift_idea_id
  , 'n/a' as creator
from etsy-data-warehouse-dev.knowledge_base.gift_mode_semaphore_gift_idea -- for all persona related gift ideas 
); 

--create table to pull in all deliveries with gift ideas so can find impressions 
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
	where date(_partitiontime) >= last_date
	  and (beacon.event_name = "recommendations_module_delivered"
	  and (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_listings%") -- mweb/ desktop occasions
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%") -- mweb/ desktop personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_gift_idea_listings%") -- boe personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_search_listings%")) -- boe search 
)
, deliveries as (
select  ----------------- this is for mweb/ desktop occasions: mobile_web has 8 listings, desktop has 16 listings
  a._date 
  , 'gift_mode_occasions_page' as page_type
  , a.occasion_id as page_id 
  , a.gift_idea_id
  , a.visit_id
  , a.sequence_number
  , concat(a.visit_id, '-', a.sequence_number) AS unique_id
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
select  ----------------- this is for all personas: mobile_web has 4 or 6 listings, desktop has 8, boe has 6 
  _date 
  , 'gift_mode_persona' as page_type
  , a.persona_id as page_id
  , a.gift_idea_id
  , a.visit_id
  , a.sequence_number
  , concat(a.visit_id, '-', a.sequence_number) AS unique_id
  , a.module_placement_clean
  , listing_id
from 
  all_gift_idea_deliveries a
cross join 
   unnest(split(listing_ids, ',')) as listing_id
where 
  module_placement_clean in ('gift_mode_gift_idea_listings','boe_gift_mode_gift_idea_listings')
group by all
union all
select  ----------------- this is for boe search, has 15 gift ideas with 12 listings each 
  _date 
  , 'gift_mode_search' as page_type
  , a.persona_id as page_id
  , a.gift_idea_id
  , a.visit_id
  , a.sequence_number
  , concat(a.visit_id, '-', a.sequence_number) AS unique_id
  , a.module_placement_clean
  , listing_id
from 
  all_gift_idea_deliveries a
cross join 
   unnest(split(listing_ids, ',')) as listing_id
where 
  module_placement_clean in ('boe_gift_mode_search_listings')
group by all
)
select
	v._date
	, v.platform
  , v.browser_platform
	, v.region
	, v.top_channel
	, v.is_admin_visit as admin
  , b.gift_idea_id
	, b.page_type
  , b.page_id
  , count(distinct b.listing_id) as unique_listings
  , count(distinct v.visit_id) as unique_visits
	, count(distinct b.unique_id) as total_impressions -- this is each visits specific delivery of gift ideas
  , count(listing_id) as total_listings_delivered -- will be used for listing rate
from
	etsy-data-warehouse-prod.weblog.visits v
inner join
	deliveries b
    using(_date, visit_id)
where
	v._date >= last_date
group by all
);

--create table to gather general listing + transaction data 
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

--create table to find listing views + associated persona/occasion ids from referring page events
create or replace temporary table clicks as (
with get_ref_tags as (
select
    date(_partitiontime) as _date
    , visit_id
    , sequence_number
    , beacon.event_name as event_name
    --this is for boe, pull outs module_placement and content_source_uid for gift idea deliveries 
    , (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
    , (select value from unnest(beacon.properties.key_value) where key = "content_source_uid") as content_source_uid
    , (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
--this is all for web, pulls out gift idea + persona id from loc on listing page
    , beacon.loc as loc
    , (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id
    , regexp_substr(beacon.loc, "gift_idea_id=([^*&?%]+)") as web_gift_idea_id-- grabs gift idea
    , regexp_substr(beacon.loc, "persona_id=([^*&?%]+)") as web_persona_id -- grabs persona_id, need to pull persona_id here bc gift_idea_ids are NOT unique to personas
    , split(regexp_substr(beacon.loc, "ref=([^*&?%]+)"), "-")[safe_offset(0)] as page_type	-- exists on web only
from 
  etsy-visit-pipe-prod.canonical.visit_id_beacons 
where 
  date(_partitiontime) >= last_date
  and ((beacon.event_name in ('view_listing') and beacon.loc like ('%gift_idea_id%')) -- comes from gift mode, for web
      or (beacon.event_name in ('recommendations_module_delivered') 
        and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ('boe_gift_mode_gift_idea_listings%') 
        or ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ('%gift_mode_search_listings%')))))
), web_clicks as (
select
	b._date
	, b.visit_id
  , case
      when b.page_type in ('gm_gift_idea_listings') then 'gift_mode_persona'
      when b.page_type in ('gm_occasion_gift_idea_listings') then 'gift_mode_occasions_page'
    end as page_type
  , b.web_gift_idea_id as gift_idea_id
  , case 
      when b.web_persona_id is not null then web_persona_id
      else cast(c.other_id as string) -- can join gift_idea_ids to occasion_ids since gift_idea_ids ARE unique to occasion_ids
    end as page_id 
  , safe_cast(b.listing_id as int64) as listing_id
  , coalesce(count(b.listing_id),0) as n_listing_views
  , coalesce(max(a.purchased_after_view),0) as purchased_after_view
from
	etsy-data-warehouse-prod.analytics.listing_views a
inner join -- only looks at the listings 
	get_ref_tags b 
    on a._date=b._date
    and a.visit_id=b.visit_id
    and a.listing_id=safe_cast(b.listing_id as int64)
    and a.sequence_number=b.sequence_number
left join 
  etsy-data-warehouse-prod.etsy_aux.gift_mode_gift_idea_relation c
    on b.web_gift_idea_id=safe_cast(c.gift_idea_id as string)
where
	a._date >= last_date
  and b.event_name in ('view_listing')
  and a.platform in ('mobile_web','desktop')
  and b.page_type in ('gm_occasion_gift_idea_listings','gm_gift_idea_listings')
group by all
), boe_agg as (
select
   _date
  , visit_id
  , sequence_number
  , case
      when module_placement_clean in ('boe_gift_mode_gift_idea_listings') then 'gift_mode_persona'
      when module_placement_clean in ('boe_gift_mode_search_listings') then 'gift_mode_search'
      else 'error'
    end as page_type
  , concat(module_placement,'-', content_source_uid) as boe_ref
  , persona_id
  , gift_idea_id
  , listing_id
from 
  get_ref_tags
cross join unnest(split(listing_ids, ",")) listing_id
where 
  event_name in ('recommendations_module_delivered') 
  and module_placement_clean is not null 
)
, boe_listing_views as (
select 
	a._date 
	, a.visit_id 
	, a.sequence_number 
	, safe_cast(a.listing_id as int64) as listing_id 
	, regexp_replace(regexp_substr(e.referrer, "ref=([^*&?%|]+)"), '-[^-]*$', '') AS boe_ref -- need it this way to get content uid
  , coalesce(count(a.listing_id),0) as n_listing_views
  , coalesce(max(a.purchased_after_view),0) as purchased_after_view
from 
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  `etsy-data-warehouse-prod`.weblog.events e 
    on a.listing_id=cast(e.listing_id as int64)
    and a.visit_id=e.visit_id
    and a.sequence_number=e.sequence_number
where 
	a._date >= last_date
  and (e.referrer like ('%boe_gift_mode_gift_idea_listings%') or e.referrer like ('%gift_mode_search_listings%'))
  and a.platform in ('boe')
group by all 
)
, boe_clicks as (
select 
a.visit_id
  , a._date
  , a.listing_id
  , b.gift_idea_id
  , b.page_type
  , b.persona_id
  , a.n_listing_views
  , a.purchased_after_view
from 
	boe_listing_views a
left join 
	boe_agg b
		on a.visit_id=b.visit_id
		and a.listing_id=safe_cast(b.listing_id as int64)
		and a.boe_ref=b.boe_ref
group by all 
)
, clicks_agg as (
select 
 _date
  , visit_id
  , listing_id
  , gift_idea_id
  , page_type
  , persona_id as page_id
  , n_listing_views
  , purchased_after_view
from 
	boe_clicks
union all
select
	_date
	, visit_id
  , listing_id
  , gift_idea_id
  , page_type
  , page_id
  , n_listing_views
  , purchased_after_view
from web_clicks
)
select
 a._date
  , a.gift_idea_id
  , c.platform
  , c.browser_platform
  , c.region
	, c.top_channel
	, c.is_admin_visit as admin
  , a.page_type
  , a.page_id
  , count(distinct a.visit_id) as visits_with_a_click
  , sum(a.n_listing_views) as total_listing_views
  , count(distinct a.listing_id) as unique_listings_viewed
  , count(distinct transaction_id) as unique_transactions
  , sum(a.purchased_after_view) as total_purchased_listings
  , coalesce(sum(b.trans_gms_net),0) as attr_gms
from 
  clicks_agg a
left join
  listing_gms b
on
  a._date = b._date
  and a.visit_id = b.visit_id
  and a.listing_id = b.listing_id
  and a.purchased_after_view > 0 -- this means there must have been a purchase 
inner join 
  etsy-data-warehouse-prod.weblog.visits c
    on a._date = c._date
    and a.visit_id = c.visit_id
where c._date >=last_date
group by all
);

insert into `etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats` (
select 
	a._date
	, a.platform
  , a.browser_platform
	, a.region
	, a.top_channel
	, a.admin
  , a.gift_idea_id
  , e.name as gift_idea_title
  , e.creator
  , a.page_type as delivery_page
  , case 
      when c.name is not null then c.name
      when d.slug is not null then initcap(replace(d.slug,'-',' '))
      else 'error'
    end as delivery_title
  -- impression metrics
  , coalesce(unique_listings,0) as unique_listings_delivered
  , coalesce(unique_visits,0) as unique_visits
	, coalesce(total_impressions,0) as total_impressions 
  , coalesce(total_listings_delivered,0) as total_listings_delivered
  -- listing view metrics 
  , coalesce(visits_with_a_click,0) as visits_with_a_click
  , coalesce(total_listing_views,0) as total_listing_views
  , coalesce(unique_listings_viewed,0) as unique_listings_viewed
  , coalesce(unique_transactions,0) as unique_transactions
  , coalesce(total_purchased_listings,0) as total_purchased_listings
  , coalesce(attr_gms,0) as attr_gms
from 
	rec_mod a -- only looks at deliveries from occasion + persona pages 
left join 
	clicks b  -- only looks at listing views associated with occasion + persona pages 
    on a._date = b._date 
    and a.gift_idea_id = b.gift_idea_id
    and a.page_id = b.page_id
    and a.platform=b.platform
    and a.page_type=b.page_type
    and a.browser_platform=b.browser_platform
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
  gift_idea_names e
    on a.gift_idea_id= e.gift_idea_id
    ---later, will be able to use etsy-data-warehouse-prod.etsy_aux.gift_mode_gift_idea_relation as source of truth beyond next few weeks
);

END
