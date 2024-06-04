-------------------------------------------------------------------------
--CLICK RATE OF EDITORIAL CONTENT ON GIFT MODE PAGES AT THE MODULE LEVEL
--how do modules perform against non-ep modules?
-------------------------------------------------------------------------
----this this is correct way 
with impressions as (
select -- get all deliveries 
	date(_partitiontime) as _date
	, visit_id
	, sequence_number
	, beacon.event_name as event_name
	-- , (select value from unnest(beacon.properties.key_value) where key = 'listing_ids') as listing_ids
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
inner join  
  etsy-data-warehouse-prod.weblog.visits b
    using (visit_id)
where
	date(_partitiontime) >= current_date-15
  and ((beacon.event_name in ('gift_mode_persona', 'gift_mode_occasions_page','popular_gift_listings_delivered') -- using primary pages bc ep delivery event on occasions page does not exist
      or (beacon.event_name in ('recommendations_module_delivered') and ((select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('gift_mode_occasion_gift_idea_%') or (select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('gift_mode_gift_idea_listings%')))))
  and b._date >= current_date-15
  and b.platform in ('mobile_web','desktop') 
)
, impressions_agg as (
select
  _date
  , visit_id
  , count(case when event_name in ('gift_mode_persona') then visit_id end) as persona_impressions
  , count(case when event_name in ('gift_mode_occasions_page') then visit_id end) as occasion_impressions
  , count(case when event_name in ('popular_gift_listings_delivered') then visit_id end) as home_impressions
  , count(case when event_name in ('recommendations_module_delivered') then visit_id end) as gift_idea_impressions
from impressions 
group by all 
)
, listing_views as
(select 
  _date
  , visit_id
  , listing_id
  , ref_tag
  , count(*) as n_listing_views
from 
  `etsy-data-warehouse-prod`.analytics.listing_views lv
where 
  lv.platform in ("desktop", "mobile_web") 
  and (ref_tag like "gm_occasions_etsys_picks%" or ref_tag like ('gm_editorial_listings%')  or ref_tag like ('gm_popular_gift_listings%') or ref_tag like ('gm_popular_gift_listings%') or ref_tag like ('gm_gift_idea_listings%') or ref_tag like ('gm_occasion_gift_idea_listings%'))
  and _date >= current_date-15
group by all
)
, listing_views_agg as (
select 
  _date 
  , visit_id 
  , sum(case when ref_tag like ('gm_occasions_etsys_picks%') then n_listing_views end) as occasion_ep_views
  , sum(case when ref_tag like ('gm_editorial_listings%') then n_listing_views end) as persona_ep_views
  , sum(case when ref_tag like ('gm_popular_gift_listings%') then n_listing_views end) as home_ep_views
  , sum(case when ref_tag like ('gm_gift_idea_listings%') or ref_tag like ('gm_occasion_gift_idea_listings%') then n_listing_views end) as gift_idea_views
from listing_views
group by all
)
select
  sum(persona_impressions) as persona_impressions
  , sum(occasion_impressions) as occasion_impressions
  , sum(home_impressions) as home_impressions
  , sum(gift_idea_impressions) as gift_idea_impressions
  , sum(occasion_ep_views) as occasion_ep_views
  , sum(persona_ep_views) as persona_ep_views
  , sum(home_ep_views) as home_ep_views
  , sum(gift_idea_views) as gift_idea_views
from impressions_agg a
left join listing_views_agg b
  using (_date, visit_id) 

	---ignore below
with impressions as (
select
	date(_partitiontime) as _date
	, visit_id
	, sequence_number
	, beacon.event_name as event_name
	-- , (select value from unnest(beacon.properties.key_value) where key = 'listing_ids') as listing_ids
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
inner join  
  etsy-data-warehouse-prod.weblog.visits b
    using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and ((beacon.event_name in ('gift_mode_persona', 'gift_mode_occasions_page','popular_gift_listings_delivered') -- using primary pages bc ep delivery event on occasions page does not exist
      or (beacon.event_name in ('recommendations_module_delivered') and ((select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('%gift_mode_occasion_gift_idea_%') or (select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('%gift_mode_gift_idea_listings%')))))
  and b._date >= current_date-30
  and b.platform in ('mobile_web','desktop') 
)
, clicks as (
select
	date(_partitiontime) as _date
	, visit_id
	, beacon.event_name as event_type
	, sequence_number
	, regexp_substr(beacon.loc, 'ref=([^*&?%]+)') as ref_tag
  , split(regexp_substr(beacon.loc, 'ref=([^*&?%]+)'), "-")[safe_offset(0)] as ref_tag_clean
  -- , (select value from unnest(beacon.properties.key_value) where key = 'listing_id') as listing_id
	from
		`etsy-visit-pipe-prod`.canonical.visit_id_beacons
	where 
    date(_partitiontime) >= current_date-30
	  and beacon.event_name in ('view_listing')
    and (regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_editorial_listings%') -- persona page ep clicks
        or regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_occasions_etsys_picks%')
        or regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_popular_gift_listings%')
        or regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_occasion_gift_idea_listings%')
        or regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_gift_idea_listings%'))
)
select
  count(case when a.event_name in ('gift_mode_persona') then a.visit_id end) as ep_persona_impressions
  , count(case when b.ref_tag_clean in ('gm_editorial_listings') then b.visit_id end) as ep_persona_clicks
  , count(case when a.event_name in ('gift_mode_occasions_page') then a.visit_id end) as ep_occasion_impressions
  , count(case when b.ref_tag_clean in ('gm_occasions_etsys_picks') then b.visit_id end)  as ep_occasion_clicks
  , count(case when a.event_name in ('popular_gift_listings_delivered') then a.visit_id end) as ep_home_impressions
  , count(case when b.ref_tag_clean in ('gm_popular_gift_listings') then b.visit_id end)  as ep_home_clicks
    , count(case when a.event_name in ('recommendations_module_delivered') then a.visit_id end) as non_ep_impressions
  , count(case when b.ref_tag_clean in ('gm_occasion_gift_idea_listings','gm_gift_idea_listings') then b.visit_id end)  as non_ep_clicks
from 
  impressions a
left join 
  clicks b
    using (_date, visit_id)

--------------------------------------------------------------------------------------------------------------------------------------------------
--CLICK RATE OF STASH LISTINGS VS OTHER LISTINGS
-- looking at the listings delivered in gift ideas, do stash listings perform better than other listings?
--------------------------------------------------------------------------------------------------------------------------------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.active_stash_listings as ( -- get all stash listings
-- select 
--   distinct l.listing_id,
-- from 
--   `etsy-data-warehouse-prod`.listing_mart.listings as l
-- left outer join 
--   `etsy-data-warehouse-prod`.etsy_shard.merch_listings m
--     using (listing_id)
-- where  
--   m.status = 0
--   and l.is_active = 1
-- );

with all_gift_idea_deliveries as ( -- gg
	select
  date(_partitiontime) as _date
  , visit_id
  , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-15
	  and beacon.event_name = "recommendations_module_delivered"
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_%") 
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%")
))
, clean_deliveries as ( -- clean up gift idea listing delivery
select 
  a._date 
  , listing_id
  , count(visit_id) as deliveries 
from 
  all_gift_idea_deliveries a
cross join 
   unnest(split(listing_ids, ',')) as listing_id
group by all 
)
, ref_tags as ( -- get listings that have a gift_idea related ref tag
select
  date(a._partitiontime) as _date
  , a.visit_id
  , (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`a
where 
  beacon.event_name in ('view_listing')
  and (regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_occasion_gift_idea_listings%')
       or regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_gift_idea_listings%')) -- comes from gift mode 
  and date(_partitiontime) >= current_date-15
)
, ref_tags_agg as (
select 
   _date
  , listing_id
  , count(visit_id) as views 
from ref_tags
group by all 
)
select 
	sum(a.deliveries) as deliveries
	, sum(b.views) as views
	, sum(case when c.listing_id is not null then a.deliveries end) as stash_deliveries
	, sum(case when c.listing_id is not null then b.views end) as stash_views
from 
	clean_deliveries a
left join 
	ref_tags_agg b
		using (listing_id, _date)
left join 
	etsy-data-warehouse-dev.madelinecollins.active_stash_listings c 
		on a.listing_id=cast(c.listing_id as string)
