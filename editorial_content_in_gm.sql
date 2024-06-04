-------------------------------------------------------------------------
--CLICK RATE OF EDITORIAL CONTENT ON GIFT MODE PAGES AT THE MODULE LEVEL
-------------------------------------------------------------------------
with impressions as (
select
	date(_partitiontime) as _date
	, visit_id
	, sequence_number
	, beacon.event_name as event_name
	-- , (select value from unnest(beacon.properties.key_value) where key = 'listing_ids') as listing_ids
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
	date(_partitiontime) >= current_date-2
and
	beacon.event_name in ('persona_editorial_listings_delivered' -- persona page etsy picks
                        , 'gift_occasion_etsys_picks_delivered') -- persona page etsy picks
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
    date(_partitiontime) >= current_date-2
	  and beacon.event_name in ('view_listing')
    and (regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_editorial_listings%') -- persona page ep clicks
        or regexp_substr(beacon.loc, 'ref=([^*&?%]+)') like ('gm_occasions_etsys_picks%')) -- occasion page ep clicks 
)
select
  count(case when b.ref_tag in ('gm_editorial_listings') then b.visit_id end)/ count(case when a.event_name in ('persona_editorial_listings_delivered') then a.visit_id end) as ep_persona_click_rate
  , count(case when b.ref_tag in ('gift_occasion_etsys_picks_delivered') then b.visit_id end)/ count(case when a.event_name in ('gm_occasions_etsys_picks') then a.visit_id end) as ep_occasion_click_rate
from 
  impressions a
left join 
  clicks b
    using (_date, visit_id)

--------------------------------------------------------------------------------------------------------------------------------------------------
--CLICK RATE OF EDITORIAL CONTENT ON GIFT MODE PAGES AT THE LISTING LEVEL (TO FIND CLICK RATE OF STASH LISTINGS VS OTHER LISTINGS)
--------------------------------------------------------------------------------------------------------------------------------------------------

