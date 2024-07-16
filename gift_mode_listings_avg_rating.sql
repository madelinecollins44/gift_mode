___________________________________________________
BUILD QUERY-- shop level reviews 
___________________________________________________
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

--gets shop_ids for all the listings, will be use to find avg rating score next 
, delivered_shops as (
select 
  a.listing_id
  , a.visit_id
  , b.shop_id
from 
  all_listings a
left join 
  etsy-data-warehouse-prod.listing_mart.listings b using (listing_id)
)

--gets avg shop review score for shops that have been delivered in gift mode in last year
, shop_ratings as (
select
  str.shop_id
  , avg(safe_cast(str.rating as numeric)) as avg_rating
from 
  delivered_shops l
left join etsy-data-warehouse-prod.etsy_shard.shop_transaction_review str 
	on str.shop_id = l.shop_id 
where
	is_deleted = 0
  and create_date > unix_seconds(timestamp(date_sub(current_date, interval 1 year))) -- reviews from the last year 
group by all
)

  --bring it all together
select 
  round(b.avg_rating) as rounded_rating
  , count(distinct a.listing_id) as unique_listings_delivered
  , count(distinct a.shop_id) as unique_shops_delivered
  , count(distinct b.shop_id) as unique_shops_ratings
  , count(visit_id) as deliveries
from
  delivered_shops a
left join 
  shop_ratings b
    using (shop_id)
group by all


-------find avg score for all listings 
with all_data as (
select
  shop_id
  , listing_id
  , safe_cast(rating as numeric) as rating
from 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review 
where create_date > unix_seconds(timestamp(date_sub(current_date, interval 1 year)))
)
, avg_score as (
select
  shop_id
  , avg(rating) as avg_rating
from all_data
group by all 
)
select
  round(avg_rating) as rounded_rating
  , count(distinct shop_id) as unique_shops
  , count(distinct listing_id) as unique_listings
from 
  avg_score
left join 
  all_data 
  using (shop_id)
group by all 


---all shops without reviews
with shops_with_ratings as (
select
  shop_id
  , safe_cast(rating as numeric) as rating
from 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review 
where create_date > unix_seconds(timestamp(date_sub(current_date, interval 1 year)))
)
, all_shops as (
select
  shop_id
  , listing_id
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics
)
, avg_score as (
select
  shop_id
  , avg(rating) as avg_rating
from shops_with_ratings
group by all 
)
select
  round(avg_rating) as rounded_rating
  , count(distinct a.shop_id) as unique_shops
  , count(distinct a.listing_id) as unique_listings
from 
  all_shops a
left join 
  avg_score b
  using (shop_id)
group by all 
	
___________________________________________________
TESTING SHOP LEVEL
___________________________________________________
select
  shop_id, 
  avg(safe_cast(rating as numeric)) as avg_rating
from 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review str 
where
	is_deleted = 0
  and create_date > unix_seconds(timestamp(date_sub(current_date, interval 1 year)))
  and shop_id in (38941705,15554482,26263376,33878037,12361451)
group by all

------listings, shops without shop reviews
--1361784585, 	



	
___________________________________________________
BUILD QUERY-- listing level reviews 
___________________________________________________
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
  -- round(b.avg_rating) as rounded_rating
  case 
    when b.listing_id is null then 'not_purchased'
    when round(b.avg_rating) is null then 'not_reviewed'
    when round(b.avg_rating) =1 then '1'
    when round(b.avg_rating) =2 then '2' 
    when round(b.avg_rating) =3 then '3'
    when round(b.avg_rating) =4 then '4'
    when round(b.avg_rating) =5 then '5'
  end as rounded_rating
  , count(distinct a.listing_id) as unique_listings
  , count(visit_id) as deliveries
from
  all_listings a
left join 
  reviews  b
    using (listing_id)
group by all 



 -----------find share across all listing reviews
 with agg as (select
  listing_id
  , round(avg(safe_cast(rating as int64))) as avg_rating
from etsy-data-warehouse-prod.quality.transaction_reviews
group by all 
)
select
round(avg_rating) as avg_rating
, count(distinct listing_id)
from agg 
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
-- , reviews as (
-- select
--   listing_id
--   , avg(safe_cast(rating as int64)) as avg_rating
-- from etsy-data-warehouse-prod.quality.transaction_reviews
-- group by all 
-- )

  --bring it all together
select 
  count(distinct a.listing_id) as delivered_listings
  , count(distinct b.listing_id) as purchased_listings
  , count(distinct case when b.listing_id is null then a.listing_id end) as delivered_not_purchased
  , count(a.visit_id) as deliveries
from
  all_listings a
left join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions   b
    using (listing_id)
group by all 
--

--listings without transactions examples
------ 949140370
------ 719757659
------ 698825500
------ 1260332210
------ 1282150710
------ 1278931530
------ 1093461096
