-------------------------------------------------------------------------
--CLICK RATE OF EDITORIAL CONTENT ON GIFT MODE PAGES AT THE MODULE LEVEL
--how does modules perform against non-ep modules?
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
	date(_partitiontime) >= current_date-30
  and beacon.event_name in ('gift_mode_persona', 'gift_mode_occasions_page','popular_gift_listings_delivered') -- using primary pages bc ep delivery event on occasions page does not exist
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
        or regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_popular_gift_listings%'))
)
select
  count(case when a.event_name in ('gift_mode_persona') then a.visit_id end) as ep_persona_impressions
  , count(case when b.ref_tag_clean in ('gm_editorial_listings') then b.visit_id end) as ep_persona_clicks
  , count(case when a.event_name in ('gift_mode_occasions_page') then a.visit_id end) as ep_occasion_impressions
  , count(case when b.ref_tag_clean in ('gm_occasions_etsys_picks') then b.visit_id end)  as ep_occasion_clicks
  , count(case when a.event_name in ('popular_gift_listings_delivered') then a.visit_id end) as ep_home_impressions
  , count(case when b.ref_tag_clean in ('gm_popular_gift_listings') then b.visit_id end)  as ep_home_clicks
from 
  impressions a
left join 
  clicks b
    using (_date, visit_id)

--------------------------------------------------------------------------------------------------------------------------------------------------
--CLICK RATE OF STASH LISTINGS VS OTHER LISTINGS
-- looking at the listings delivered in gift ideas, do stash listings perform better than other listings?
--------------------------------------------------------------------------------------------------------------------------------------------------

