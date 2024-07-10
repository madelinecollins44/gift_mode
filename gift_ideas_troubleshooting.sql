
******NARROWED IT DOWN TO ISSUE BEING IN FINAL JOIN -- test indivial gift idea to see where issue is makes sense so must be in join
select page_type, sum(total_impressions) from etsy-bigquery-adhoc-prod._script50c2b9b7c20584e70ce84eac7c2f99ab89ae89e9.rec_mod
 where gift_idea_id in ('fb3dc615-a696-4605-957f-affcbf5256f9')
and _date= '2024-07-07' group by all
	--persona: 3338
	--search: 252

select delivery_page, sum(total_impressions) from etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats
 where gift_idea_id in ('fb3dc615-a696-4605-957f-affcbf5256f9')
and _date= '2024-07-07' group by all
	--persona: 20460
	--search: 1512
with beacons_table_raw as (
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
  , concat(visit_id, '-', sequence_number) AS unique_id
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
		, (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id 
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
    , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-10
	  and (beacon.event_name = "recommendations_module_delivered")
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_listings%") -- mweb/ desktop occasions
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%") -- mweb/ desktop personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_gift_idea_listings%") -- boe personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_search_listings%")) -- boe search 
)
, beacons_table as (
select
  gift_idea_id
  , _date
  , count(distinct unique_id) AS total_impressions
    , case 
      when module_placement_clean in ('gift_mode_occasion_gift_idea_listings') then 'gift_mode_occasions_page' 
      when module_placement_clean in ('gift_mode_gift_idea_listings','boe_gift_mode_gift_idea_listings') then 'gift_mode_persona' 
      when module_placement_clean in ('boe_gift_mode_search_listings') then 'gift_mode_search' 
      else 'error'
      end as delivery_page
from beacons_table_raw
 where gift_idea_id in ('aed9f86b-edca-4d14-8695-6b1798245dcf')
and _date >= current_date-2 
and _date != current_date
group by all
)
select _date, gift_idea_id, delivery_page, sum(total_impressions) from beacons_table group by all
-- select 
-- -- impressions persona 3159 7/9
-- -- impressions gift_mode_search 249 7/9

	select _date, gift_idea_title, delivery_page, sum(total_impressions) from etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats
 where gift_idea_id in ('aed9f86b-edca-4d14-8695-6b1798245dcf')
and _date >= current_date-2  group by all


---checked + confirmed these raw numbers match what is in looker 
	with beacons_table_raw as (
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
  , concat(visit_id, '-', sequence_number) AS unique_id
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
		, (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id 
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
    , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-10
	  and (beacon.event_name = "recommendations_module_delivered")
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_listings%") -- mweb/ desktop occasions
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%") -- mweb/ desktop personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_gift_idea_listings%") -- boe personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_search_listings%")) -- boe search 
)
, beacons_table as (
select
  gift_idea_id
  , _date
  , count(distinct unique_id) AS total_impressions
    , case 
      when module_placement_clean in ('gift_mode_occasion_gift_idea_listings') then 'gift_mode_occasions_page' 
      when module_placement_clean in ('gift_mode_gift_idea_listings','boe_gift_mode_gift_idea_listings') then 'gift_mode_persona' 
      when module_placement_clean in ('boe_gift_mode_search_listings') then 'gift_mode_search' 
      else 'error'
      end as delivery_page
from beacons_table_raw
 where gift_idea_id in ('22ed2427-d820-45a2-aafc-859dc249d620','81716753-3264-44e0-b56d-8be4fbaa6929','fb3dc615-a696-4605-957f-affcbf5256f9')
and _date = '2024-07-08'
group by all
)
select gift_idea_id, delivery_page, sum(total_impressions) from beacons_table group by all
-- select 
-- -- impressions persona 3159 7/9
-- -- impressions gift_mode_search 249 7/9

________________________________________________________
--check to make sure raw data from beacons table matches what is getting put out in rollup
________________________________________________________
with current_rollup as (
select
  gift_idea_id
  , sum(total_impressions) as total_impressions
from 
  etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats
	--  etsy-bigquery-adhoc-prod._script7065f0ce6d076ab97aea44909a86cf9dd0339f2a.rec_mod --> use this table instead to see where impressions are getting messed up 
where _date >= current_date-2
group by all
)
, beacons_table_raw as (
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
  , concat(visit_id, '-', sequence_number) AS unique_id
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
	  and (beacon.event_name = "recommendations_module_delivered")
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_listings%") -- mweb/ desktop occasions
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%") -- mweb/ desktop personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_gift_idea_listings%") -- boe personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_search_listings%")) -- boe search 
)
, beacons_table as (
select
  gift_idea_id
  , count(distinct unique_id) as total_impressions
from beacons_table_raw
group by all
)
select
  a.gift_idea_id
  , a.total_impressions as beacons_
  , b.total_impressions as rollup_
  , a.total_impressions-b.total_impressions as difference
from beacons_table a
left join current_rollup b using (gift_idea_id)
group by all order by 3 desc
  
________________________________________________________
--make sure each cte is unique on all dimensions 
	--yes: all tables have 1 count
________________________________________________________
select 
	_date
	, platform
  , browser_platform
	, region
	, top_channel
	, admin
  , gift_idea_id
	, page_type
  , page_id
  , count(*)
from 
  etsy-bigquery-adhoc-prod._script7065f0ce6d076ab97aea44909a86cf9dd0339f2a.rec_mod
	--  etsy-bigquery-adhoc-prod._script7065f0ce6d076ab97aea44909a86cf9dd0339f2a.clicks
group by all order by count(*) desc



	etsy-bigquery-adhoc-prod._script7065f0ce6d076ab97aea44909a86cf9dd0339f2a.clicks

________________________________________________________
--see if all days are higher or just certain days 
________________________________________________________
--test gift idea impressions over time
select _date, sum(total_impressions) as total_impressions from etsy-data-warehouse-prod.rollups.gift_mode_gift_idea_stats
group by all order by 1 desc

--test persona pageviews over time
select _date, sum(all_pageviews) as all_pageviews from etsy-data-warehouse-prod.rollups.gift_mode_persona_stats
group by all order by 1 desc

----the impressions are very different over time, more changes than persona pageview 


________________________________________________________
--test indivial gift idea to see where issue is
________________________________________________________
--what the number should be, from beacons table 
with beacons_table_raw as (
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
  , concat(visit_id, '-', sequence_number) AS unique_id
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
		, (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id 
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
    , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-10
	  and (beacon.event_name = "recommendations_module_delivered")
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_listings%") -- mweb/ desktop occasions
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%") -- mweb/ desktop personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_gift_idea_listings%") -- boe personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_search_listings%")) -- boe search 
)
-- , beacons_table as (
select
  gift_idea_id
  , _date
  , count(distinct concat(visit_id, '-', sequence_number)) AS unique_id
    , case 
      when module_placement_clean in ('gift_mode_occasion_gift_idea_listings') then 'gift_mode_occasions_page' 
      when module_placement_clean in ('gift_mode_gift_idea_listings','boe_gift_mode_gift_idea_listings') then 'gift_mode_persona' 
      when module_placement_clean in ('boe_gift_mode_search_listings') then 'gift_mode_search' 
      else 'error'
      end as delivery_page
from beacons_table_raw
where gift_idea_id in ('fb3dc615-a696-4605-957f-affcbf5256f9')
and _date= '2024-07-07'
group by all
)
select 
--3338 impressions persona

--what is coming from cte 
with all_gift_idea_deliveries as (
	select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
      , concat(visit_id, '-', sequence_number) AS unique_id
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean
		, (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id 
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
    , (select value from unnest(beacon.properties.key_value) where key = "occasion_id") as occasion_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where date(_partitiontime) >= current_date-10
	  and (beacon.event_name = "recommendations_module_delivered")
	  and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_occasion_gift_idea_listings%") -- mweb/ desktop occasions
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("gift_mode_gift_idea_listings%") -- mweb/ desktop personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_gift_idea_listings%") -- boe personas
        or (select value from unnest(beacon.properties.key_value) where key = "module_placement") like ("boe_gift_mode_search_listings%")) -- boe search 
)
, deliveries as (
select 
  a._date 
  , case 
      when module_placement_clean in ('gift_mode_occasion_gift_idea_listings') then 'gift_mode_occasions_page' 
      when module_placement_clean in ('gift_mode_gift_idea_listings','boe_gift_mode_gift_idea_listings') then 'gift_mode_persona' 
      when module_placement_clean in ('boe_gift_mode_search_listings') then 'gift_mode_search' 
      else 'error'
    end as page_type
  , coalesce(a.occasion_id, a.persona_id) as page_id
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
group by all
) 
, agg as (select
	b._date
	, v.platform
  , v.browser_platform
	, v.region
	, v.top_channel
	, v.is_admin_visit as admin
  , b.gift_idea_id
	, b.page_type
  , b.page_id
  , count(distinct b.listing_id) as unique_listings
  , count(distinct b.visit_id) as unique_visits
	, count(distinct b.unique_id) as total_impressions -- this is each visits specific delivery of gift ideas
  , count(b.listing_id) as total_listings_delivered -- will be used for listing rate
from 
	etsy-data-warehouse-prod.weblog.visits v
left join 
	deliveries b
    using(_date, visit_id)
where
	v._date >= current_date-10
group by all
)
select page_type, sum(total_impressions) from agg where gift_idea_id in ('fb3dc615-a696-4605-957f-affcbf5256f9')
and _date= '2024-07-07' group by all
--3338 impressions persona
-------THESE MATCH--ISSUE MUST BE IN JOIN 



________________________________________________________
--TESTING CLICKS 
________________________________________________________
---web
with get_ref_tags as (
select
    date(_partitiontime) as _date
    , visit_id
    , sequence_number
    , beacon.event_name as event_name
    --this is for boe, pull outs module_placement and content_source_uid for gift idea deliveries 
    , (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean -- this will be used as page type for boe
    , (select value from unnest(beacon.properties.key_value) where key = "content_source_uid") as content_source_uid
    , (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
--this is all for web, pulls out gift idea + persona id from loc on listing page
    , beacon.loc as loc
    , (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id
    , regexp_substr(beacon.loc, "gift_idea_id=([^*&?%]+)") as web_gift_idea_id-- grabs gift idea
    , regexp_substr(beacon.loc, "persona_id=([^*&?%]+)") as web_persona_id -- grabs persona_id, need to pull persona_id here bc gift_idea_ids are NOT unique to personas
    , split(regexp_substr(beacon.loc, "ref=([^*&?%]+)"), "-")[safe_offset(0)] as page_type
from 
  etsy-visit-pipe-prod.canonical.visit_id_beacons 
where 
  date(_partitiontime) >= current_date-5
  and ((beacon.event_name in ('view_listing') and beacon.loc like ('%gift_idea_id%')) -- comes from gift mode, for web
      or (beacon.event_name in ('recommendations_module_delivered') 
        and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ('boe_gift_mode_gift_idea_listings%') 
        or ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ('%gift_mode_search_listings%')))))
)
  select _date, count(listing_id) from get_ref_tags
 where web_gift_idea_id in ('aed9f86b-edca-4d14-8695-6b1798245dcf')
  and event_name in ('view_listing')
  and page_type in ('gm_occasion_gift_idea_listings','gm_gift_idea_listings')
  group by all	
-- 2024-07-08:10
-- 2024-07-09:8
-- 2024-07-07: 8	
-- 2024-07-10:4	
-- 2024-07-05:10	
-- 2024-07-06:3


-- ), web_clicks as (
-- select
-- 	b._date
-- 	, b.visit_id
--   , case
--       when b.page_type in ('gm_gift_idea_listings') then 'gift_mode_persona'
--       when b.page_type in ('gm_occasion_gift_idea_listings') then 'gift_mode_occasions_page'
--     end as page_type
--   , b.web_gift_idea_id as gift_idea_id
--   , case 
--       when b.web_persona_id is not null then web_persona_id
--       else cast(c.other_id as string) -- can join gift_idea_ids to occasion_ids since gift_idea_ids ARE unique to occasion_ids
--     end as page_id 
--   , safe_cast(b.listing_id as int64) as listing_id
--   , coalesce(count(b.listing_id),0) as n_listing_views
--   , coalesce(max(a.purchased_after_view),0) as purchased_after_view
-- from
-- 	etsy-data-warehouse-prod.analytics.listing_views a
-- inner join -- only looks at the listings 
-- 	get_ref_tags b 
--     on a._date=b._date
--     and a.visit_id=b.visit_id
--     and a.listing_id=safe_cast(b.listing_id as int64)
--     and a.sequence_number=b.sequence_number
-- left join 
--   etsy-data-warehouse-prod.etsy_aux.gift_mode_gift_idea_relation c
--     on b.web_gift_idea_id=safe_cast(c.gift_idea_id as string)
-- where
-- 	a._date >= current_date-5
--   and b.event_name in ('view_listing')
--   and a.platform in ('mobile_web','desktop')
--   and b.page_type in ('gm_occasion_gift_idea_listings','gm_gift_idea_listings') -- excludes other ref_tags (others had counts of 1)
-- group by all
-- )
--   --check by finding listing views of this gift id on web
select _date, delivery_page, sum(total_listing_views) from etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats
 where gift_idea_id in ('aed9f86b-edca-4d14-8695-6b1798245dcf')
and _date >= current_date-4
and platform in ('mobile_web','desktop') group by all
-- 2024-07-07: 7	
-- 2024-07-08:10	
-- 2024-07-09:3	
-- 2024-07-06:3

-- select _date, delivery_page, sum(total_listing_views) from etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats
--  where gift_idea_id in ('aed9f86b-edca-4d14-8695-6b1798245dcf')
-- and _date >= current_date-4
-- and platform in ('mobile_web','desktop') group by all
-- 	--persona: 20460
-- 	--search: 1512

  select _date, sum(total_listing_views) from etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats 
where platform in ('mobile_web','desktop') and _date>= current_date-5  group by all order by 1 desc
--2024-07-09:4916
--2024-07-08:4462
--2024-07-07:3954
--2024-07-06:3663
--2024-07-05:3850

-- select 4916/5044 --97% accuracy 

with get_ref_tags as (
select
    date(_partitiontime) as _date
    , visit_id
    , sequence_number
    , beacon.event_name as event_name
    --this is for boe, pull outs module_placement and content_source_uid for gift idea deliveries 
    , (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean -- this will be used as page type for boe
    , (select value from unnest(beacon.properties.key_value) where key = "content_source_uid") as content_source_uid
    , (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
--this is all for web, pulls out gift idea + persona id from loc on listing page
    , beacon.loc as loc
    , (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id
    , regexp_substr(beacon.loc, "gift_idea_id=([^*&?%]+)") as web_gift_idea_id-- grabs gift idea
    , regexp_substr(beacon.loc, "persona_id=([^*&?%]+)") as web_persona_id -- grabs persona_id, need to pull persona_id here bc gift_idea_ids are NOT unique to personas
    , split(regexp_substr(beacon.loc, "ref=([^*&?%]+)"), "-")[safe_offset(0)] as page_type
from 
  etsy-visit-pipe-prod.canonical.visit_id_beacons 
where 
  date(_partitiontime) >= current_date-5
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
	a._date >= current_date-5
  and b.event_name in ('view_listing')
  and a.platform in ('mobile_web','desktop')
  and b.page_type in ('gm_occasion_gift_idea_listings','gm_gift_idea_listings') -- excludes other ref_tags (others had counts of 1)
group by all
)
select _date, sum(n_listing_views) from web_clicks 
where _date>= current_date-5 group by all order by 1 desc
--2024-07-09:5044
--2024-07-08:4589
--2024-07-07:4125
--2024-07-06:3759
--2024-07-05:3759


  select a.* 
  from web_clicks a 
  -- inner join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
 where web_gift_idea_id in ('aed9f86b-edca-4d14-8695-6b1798245dcf')
  and event_name in ('view_listing')
  and _date = '2024-07-09'
  -- and v._date>= current_date-5
  -- and platform in ('mobile_web','desktop')
  and page_type in ('gm_occasion_gift_idea_listings','gm_gift_idea_listings')
  group by all

  check by finding listing views of this gift id on web
  select _date, sum(n_listing_views) from web_clicks
 where gift_idea_id in ('aed9f86b-edca-4d14-8695-6b1798245dcf')
and _date >= current_date-2 

---boe
with get_ref_tags as (
select
    date(_partitiontime) as _date
    , visit_id
    , sequence_number
    , beacon.event_name as event_name
    --this is for boe, pull outs module_placement and content_source_uid for gift idea deliveries 
    , (select value from unnest(beacon.properties.key_value) where key = "module_placement") as module_placement
    , split((select value from unnest(beacon.properties.key_value) where key = "module_placement"), "-")[safe_offset(0)] as module_placement_clean -- this will be used as page type for boe
    , (select value from unnest(beacon.properties.key_value) where key = "content_source_uid") as content_source_uid
    , (select value from unnest(beacon.properties.key_value) where key = "gift_idea_id") as gift_idea_id
    , (select value from unnest(beacon.properties.key_value) where key = "persona_id") as persona_id
    , (select value from unnest(beacon.properties.key_value) where key = "listing_ids") as listing_ids
--this is all for web, pulls out gift idea + persona id from loc on listing page
    , beacon.loc as loc
    , (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id
    , regexp_substr(beacon.loc, "gift_idea_id=([^*&?%]+)") as web_gift_idea_id-- grabs gift idea
    , regexp_substr(beacon.loc, "persona_id=([^*&?%]+)") as web_persona_id -- grabs persona_id, need to pull persona_id here bc gift_idea_ids are NOT unique to personas
    , split(regexp_substr(beacon.loc, "ref=([^*&?%]+)"), "-")[safe_offset(0)] as page_type
from 
  etsy-visit-pipe-prod.canonical.visit_id_beacons 
where 
  date(_partitiontime) >= current_date-5
  and ((beacon.event_name in ('view_listing') and beacon.loc like ('%gift_idea_id%')) -- comes from gift mode, for web
      or (beacon.event_name in ('recommendations_module_delivered') 
        and ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ('boe_gift_mode_gift_idea_listings%') 
        or ((select value from unnest(beacon.properties.key_value) where key = "module_placement") like ('%gift_mode_search_listings%')))))
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
	, safe_cast(a.listing_id as int64) as listing_id 
	, regexp_replace(regexp_substr(e.referrer, "ref=([^*&?%|]+)"), '-[^-]*$', '') AS boe_ref -- need it this way to get content uid
  , coalesce(count(a.listing_id),0) as n_listing_views
  , coalesce(max(a.purchased_after_view),0) as purchased_after_view
from 
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  `etsy-data-warehouse-prod`.weblog.events e 
    -- on a.listing_id=cast(e.listing_id as int64)
    on a.visit_id=e.visit_id
    and a.sequence_number=e.sequence_number
where 
	a._date >= current_date-5
  and (e.referrer like ('%boe_gift_mode_gift_idea_listings%') or e.referrer like ('%gift_mode_search_listings%'))
  and a.platform in ('boe')
  and e.event_type in ('view_listing')
group by all 
)
-- select _date, sum(n_listing_views) from boe_listing_views 
-- where _date>= current_date-5 group by all order by 1 desc
--2024-07-09:28913
--2024-07-08:28440
--2024-07-07:29873
--2024-07-06:29333
--2024-07-05:25783


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
select _date, sum(n_listing_views) from boe_clicks 
where _date>= current_date-5 group by all order by 1 desc
-- --2024-07-09:28895
-- --2024-07-08:28406
-- --2024-07-07:29854
-- --2024-07-06:29292
-- --2024-07-05:25760


  select _date, sum(total_listing_views) from etsy-data-warehouse-dev.rollups.gift_mode_gift_idea_stats 
where platform in ('boe') and _date>= current_date-5  group by all order by 1 desc
--2024-07-09:28524
--2024-07-08:28106
--2024-07-07:29483
--2024-07-06:28873
--2024-07-05:25339
