_________________
BUILD QUERY
_________________
 
-- get delivered listings
with listing_deliveries as (
select
date(_partitiontime) as _date
, visit_id
, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
, split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
, (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
date(_partitiontime) >= current_date-7
and
beacon.event_name = "recommendations_module_delivered"
and (
((select value from unnest(beacon.properties.key_value) where key = "module_placement") like "gm_gift_idea_listings%") or -- web, personas
((select value from unnest(beacon.properties.key_value) where key = "module_placement") like "gm_deluxe_persona_card%") or -- web, quiz
((select value from unnest(beacon.properties.key_value) where key = "module_placement") like "gift_mode_occasion_gift_idea_listings%") or --web, occasions
((select value from unnest(beacon.properties.key_value) where key = "module_placement") like "boe_gift_mode_gift_idea_listings%") or -- boe, personas
-- ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like "boe_gift_mode_quiz_results_listings%") or -- boe, quiz
((select value from unnest(beacon.properties.key_value) where key = "module_placement") like "boe_gift_mode_search_listings%") or -- boe, search
((select value from unnest(beacon.properties.key_value) where key = "module_placement") like "boe_gift_mode_occasion_gift_idea_listings%") -- boe, occasions 
))

  -- agg delivered listings
, all_listings as (
select
_date
, safe_cast(b as int64) as listing_id
, visit_id
from
listing_deliveries
cross join
unnest((split(regexp_replace(listing_ids,"\\[|\\]", ""), ","))) b
)

  --get overall reviews, no timestamp on purchase date 
, reviews as (
select
  listing_id
  , avg(safe_cast(rating as int64)) as avg_rating
from etsy-data-warehouse-prod.quality.transaction_reviews
group by all 
)

  --bring it all together
select 
  round(b.avg_rating) as rounded_rating
  , (round(b.avg_rating)  * 2) / 2 as rounded_half_rating
  , count(distinct a.listing_id) as unique_listings
  , count(visit_id) as deliveries
from
  all_listings a
left join 
  reviews  b
    using (listing_id)
group by all 


_________________
TESTING
_________________
--how many listings have reviews? how does that compare to gift mode share?
  with agg as (
select 
listing_id 
, sum(has_review) as reviews
from etsy-data-warehouse-prod.quality.transaction_reviews
group by all
)
select
  count(distinct case when reviews > 0 then listing_id end) as listings_with_reviews
  ,  count(distinct case when reviews = 0 then listing_id end) as listings_without_reviews
  , count(distinct listing_id) as total_listings
from agg 
-- no reviews: 169633809, 55.7%
-- reviews: 134726452, 44.3%
-- total listings: 304360261

--are the listings without ratings not purchased?
-- 949140370
-- 719757659
-- 698825500
-- 1260332210
-- 1282150710
-- 1278931530
-- 1093461096
