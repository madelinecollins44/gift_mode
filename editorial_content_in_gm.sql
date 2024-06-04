-------------------------------------------------------------------------
--CLICK RATE OF EDITORIAL CONTENT ON GIFT MODE PAGES AT THE MODULE LEVEL
--how do modules perform against non-ep modules?
-------------------------------------------------------------------------

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
	date(_partitiontime) >= current_date-3
  -- and (beacon.event_name in ('gift_mode_persona', 'gift_mode_occasions_page','popular_gift_listings_delivered') -- using primary pages bc ep delivery event on occasions page does not exist
      and (beacon.event_name in ('recommendations_module_delivered') and ((select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('%gift_mode_occasion_gift_idea_%') or (select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('%gift_mode_gift_idea_listings%')))
  and b._date >= current_date-3
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
    date(_partitiontime) >= current_date-3
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
  , count(case when b.ref_tag_clean in ('gm_occasion_gift_idea_listings','gm_gift_idea_listings') then b.visit_id end)  as non_ep_home_clicks
from 
  impressions a
left join 
  clicks b
    using (_date, visit_id)


--------------------------------------------------------------------------------------------------------------------------------------------------
--CLICK RATE OF STASH LISTINGS VS OTHER LISTINGS
-- looking at the listings delivered in gift ideas, do stash listings perform better than other listings?
--------------------------------------------------------------------------------------------------------------------------------------------------
with get_stash_listings as (
select 
  distinct l.listing_id,
from 
  `etsy-data-warehouse-prod`.listing_mart.listings as l
left outer join 
  `etsy-data-warehouse-prod`.etsy_shard.merch_listings m
    using (listing_id)
where  
  m.status = 0
  and l.is_active = 1
), all_gift_idea_deliveries as ( -- get all listings delivered in gift idea modules 
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-3
	  and beacon.event_name = "recommendations_module_delivered"
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_%") -- mweb/ desktop occasions
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%") -- mweb/ desktop personas
))
, clean_deliveries as ( -- clean up gift idea listing delivery
select 
  a._date 
  , a.visit_id
  , listing_id
from 
  all_gift_idea_deliveries a
cross join 
   unnest(split(listing_ids, ',')) as listing_id
)
, ref_tags as ( -- get listings that have a gift_idea related ref tag
select
  date(a._partitiontime) as _date
  , a.visit_id
  , a.sequence_number
  , beacon.event_name as event_name
	, regexp_substr(beacon.loc, 'ref=([^*&?%]+)') as ref_tag
  , split(regexp_substr(beacon.loc, 'ref=([^*&?%]+)'), "-")[safe_offset(0)] as ref_tag_clean    
  , (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`a
where 
  beacon.event_name in ('view_listing')
  and (regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_occasion_gift_idea_listings%')
       or regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_gift_idea_listings%')) -- comes from gift mode 
  and date(_partitiontime) >= current_date-3
)
select 
	count(a.listing_id) as listing_impression
	, count(b.listing_id) as listing_click
	, count(case when c.listing_id is not null then a.listing_id end) as stash_listing_impression
	, count(case when c.listing_id is not null then b.listing_id end) as stash_listing_click
from 
	clean_deliveries a
left join 
	ref_tags b
		using (visit_id, listing_id, _date)
left join 
	get_stash_listings c 
		on a.listing_id=cast(c.listing_id as string)


