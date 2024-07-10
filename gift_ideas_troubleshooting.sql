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