---can i make a temp table with all necessary events and update there instead of inquery as more events are added?
begin

--create table with all core gift mode events
create or replace temporary table core_pages (event_name STRING);
insert into core_pages (event_name)
values
('gift_mode_home') --gift mode home, boe + web
  , ('gift_mode_persona')-- gift mode personas, boe + web 
  , ('gift_mode_occasions_page')-- gift mode occasions, web
  , ('gift_mode_browse_all_personas') -- see all personas, web
  , ('gift_mode_see_all_personas') -- see all personas, boe
  , ('gift_mode_results') -- gift mode quiz results, web
  , ('gift_mode_quiz_results') -- gift mode quiz results, boe
;

--create table with all core gift mode events
create or replace temporary table other_content (event_name STRING);
insert into other_content (event_name)
values
  ('gift_mode_shop_by_occasions_module_seen') --shop by occasion module on homepage, web
  , ('gm_gift_page_ingress_loaded') -- gift mode promo banner on gift category page, web
  , ('search_gift_mode_banner_seen') -- gift mode promo banner on search page, web
  , ('gm_hp_banner_loaded_seen') --homepage banner, web
  , ('search_gift_mode_banner_seen')-- bottom of search page for gift queries, web
  , ('gift_mode_introduction_modal_shown') --Gift Mode introduction overlay shown on homescreen, boe
;


--this table grabs visits across gift mode related content and core gift mode pages 
create or replace temporary table visits as (
  with get_recmods_events as (
  select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = 'module_placement') as module_placement
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
	where 
    date(_partitiontime) >= current_date-2
	  and ((beacon.event_name = 'recommendations_module_delivered' 
        and ((select value from unnest(beacon.properties.key_value) where key = 'module_placement') in ('lp_suggested_personas_related','homescreen_gift_mode_personas') --related personas module on listing page, web AND app home popular personas module delivered, boe
        or (select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('hub_stashgrid_module-%') --Featured personas on hub, web
    or beacon.event_name in 
    ------various ingresses + banners 
    ('gift_mode_shop_by_occasions_module_seen' --shop by occasion module on homepage, web
    , 'gm_gift_page_ingress_loaded' -- gift mode promo banner on gift category page, web
    , 'search_gift_mode_banner_seen' -- gift mode promo banner on search page, web
    , 'gm_hp_banner_loaded_seen' --homepage banner, web
    , 'search_gift_mode_banner_seen'-- bottom of search page for gift queries, web
    , 'gift_mode_introduction_modal_shown' --Gift Mode introduction overlay shown on homescreen, boe
     ------core visits 
    , 'gift_mode_home' --gift mode home, boe + web
    , 'gift_mode_persona'-- gift mode personas, boe + web 
    , 'gift_mode_occasions_page'-- gift mode occasions, web
    , 'gift_mode_browse_all_personas' -- see all personas, web
    , 'gift_mode_see_all_personas' -- see all personas, boe
    , 'gift_mode_results' -- gift mode quiz results, web
    , 'gift_mode_quiz_results'))-- gift mode quiz results, boe
)
select 
	b._date  
	, a.platform 
  , a.browser_platform 
	, a.region  
  , a.is_admin_visit as admin 
  , a.top_channel 
  , count(distinct visit_id) as visits
  , count(visit_id) as impressions
  , count(distinct case when event_name in ('gift_mode_home','gift_mode_persona','gift_mode_occasions_page','gift_mode_browse_all_personas','gift_mode_see_all_personas','gift_mode_results','gift_mode_quiz_results') then visit_id end) as core_visits
    , count(case when event_name in ('gift_mode_home','gift_mode_persona','gift_mode_occasions_page','gift_mode_browse_all_personas','gift_mode_see_all_personas','gift_mode_results','gift_mode_quiz_results') then visit_id end) as core_impressions
from 
  etsy-data-warehouse-prod.weblog.visits a
inner join 
  get_recmods_events b 
    using (_date, visit_id)
where 
  a._date >= current_date-2
group by all 
);

------------clicks + reftags
		select 
	_date 
	, visit_id 
	, sequence_number 
  -- , 
	-- , regexp_substr(e.referrer, "ref=([^*&?%|]+)") as boe_ref 
	, ref_tag
from 
	`etsy-data-warehouse-prod`.weblog.events e 
where 
	_date >= current_date-3
ref_tag in
	('hp_gm_shop_by_occasion_module' -- Shop by occasion on homepage, web
	, 'listing_suggested_personas_related' --Related personas module/ personas variant, web
	, 'hub_GiftMode' --Gift Teaser promo banner on hub, web
	, 'hp_promo_secondary_042224_US_Gifts_%' -- Onsite Promo Banner (Mother's Day/ Father's Day), web
	, 'hp_promo_tertiary_042224_US_Gifts_%' -- Onsite Promo Banner (Mother's Day/ Father's Day), web
	, 'GiftTeaser_MDAY24_Skinny_Sitewide' -- Skinny Banner (Mother's Day), web
	and (ref_tag 
			like ('listing_suggested_persona_listings_related%') --Related personas module on listing page, web
			or like ('gm%') -- general ref_tags, web
    )
    

