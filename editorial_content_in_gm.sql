-------------------------------------------------------------------------
--CLICK RATE OF EDITORIAL CONTENT ON GIFT MODE PAGES AT THE MODULE LEVEL
--how do modules perform against non-ep modules?
-------------------------------------------------------------------------
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
  , purchased_after_view
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
    , sum(case when ref_tag like ('gm_occasions_etsys_picks%') then purchased_after_view end) as occasion_ep_purchases
  , sum(case when ref_tag like ('gm_editorial_listings%') then purchased_after_view end) as persona_ep_purchases
  , sum(case when ref_tag like ('gm_popular_gift_listings%') then purchased_after_view end) as home_ep_purchases
  , sum(case when ref_tag like ('gm_gift_idea_listings%') or ref_tag like ('gm_occasion_gift_idea_listings%') then purchased_after_view end) as gift_idea_purchases
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
  , sum(occasion_ep_views) as occasion_ep_purchases
  , sum(persona_ep_views) as persona_ep_purchases
  , sum(home_ep_views) as home_ep_purchases
  , sum(gift_idea_views) as gift_idea_purchases
from impressions_agg a
left join listing_views_agg b
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
  , a.sequence_number
  , (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`a
where 
  beacon.event_name in ('view_listing')
  and (regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_occasion_gift_idea_listings%')
       or regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_gift_idea_listings%')) -- comes from gift mode 
  and date(_partitiontime) >= current_date-15
)
, listing_views as (
select 
  a._date
  , a.visit_id
  , a.listing_id 
  , count(*) as n_listing_views
  , purchased_after_view
from 
  ref_tags a
inner join 
  `etsy-data-warehouse-prod`.analytics.listing_views lv
    on a._date = lv._date
    and a.visit_id = lv.visit_id
    and a.sequence_number = lv.sequence_number
    and cast(a.listing_id as int64)= lv.listing_id
where lv._date >= current_date-15
group by all 
)
, ref_tags_agg as (
select 
   _date
  , listing_id
  , sum(n_listing_views) as views
  , sum(purchased_after_view) as purchase_after_view 
from listing_views
group by all 
)
select 
	sum(a.deliveries) as deliveries
	, sum(b.views) as views
  , sum(b.purchase_after_view) as purchases
	, sum(case when c.listing_id is not null then a.deliveries end) as stash_deliveries
	, sum(case when c.listing_id is not null then b.views end) as stash_views	
  , sum(case when c.listing_id is not null then b.purchase_after_view end) as stash_purchases	
	, sum(case when c.listing_id is null then a.deliveries end) as non_stash_deliveries
	, sum(case when c.listing_id is null then b.views end) as non_stash_views
  , sum(case when c.listing_id is null then b.purchase_after_view end) as non_stash_purchases
from 
	clean_deliveries a
left join 
	ref_tags_agg b
		using (listing_id, _date)
left join 
	etsy-data-warehouse-dev.madelinecollins.active_stash_listings c 
		on a.listing_id=cast(c.listing_id as string)
