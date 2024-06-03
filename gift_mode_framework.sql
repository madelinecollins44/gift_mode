---can i make a temp table with all necessary events and update there instead of inquery as more events are added?
begin

------------------------------------------------------------------------------------------------------------
--VISITS AND IMPRESSIONS: this table grabs visits across gift mode related content and core gift mode pages 
------------------------------------------------------------------------------------------------------------
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
        and ((select value from unnest(beacon.properties.key_value) where key = 'module_placement') in ('lp_suggested_personas_related','homescreen_gift_mode_personas')) --related personas module on listing page, web AND app home popular personas module delivered, boe
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
)
);

------------------------------------
--CLICKS
------------------------------------		
create or replace temporary table clicks as (
with get_refs as (
select 
	_date 
	, visit_id 
	, sequence_number 
	-- , regexp_substr(e.referrer, "ref=([^*&?%|]+)") as boe_ref 
	, ref_tag
from 
	`etsy-data-warehouse-prod`.weblog.events e 
where 
	_date >= last_date
  and (ref_tag in ('hp_gm_shop_by_occasion_module' -- Shop by occasion on homepage, web
	    , 'listing_suggested_personas_related' --Related personas module/ personas variant, web
	    , 'hub_GiftMode' --Gift Teaser promo banner on hub, web
      , 'GiftTeaser_MDAY24_Skinny_Sitewide')) -- Skinny Banner (Mother's Day), web
	or (ref_tag like ('hp_promo_secondary_042224_US_Gifts_%')-- Onsite Promo Banner (Mother's Day/ Father's Day), web
      or ref_tag like ('hp_promo_tertiary_042224_US_Gifts_%'))-- Onsite Promo Banner (Mother's Day/ Father's Day), web
)
--how do i want to agg these? do i want to break it down by core vs all gift mode visits? 

------------------------------------
--LISTING VIEWS 
------------------------------------
create or replace temporary table listing_gms as (
select
	tv.date as _date
	, tv.visit_id
	, tv.platform_app as platform
	, tv.transaction_id
	, t.listing_id
	, tg.trans_gms_net
from
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv
join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans tg
using(transaction_id)
join
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t
on
	tv.transaction_id = t.transaction_id
where
	tv.date >= current_date-2
)
;

create or replace temporary table listing_views as (
with listing_views as (
select 
	_date 
	, visit_id 
	, sequence_number 
	, safe_cast(listing_id as int64) as listing_id
	, regexp_substr(e.referrer, "ref=([^*&?%|]+)") as boe_ref 
	, ref_tag
from 
	`etsy-data-warehouse-prod`.weblog.events e 
where 
	_date >= current_date-2
	and event_type = "view_listing"
  and ((ref_tag like ('gm_%') or ref_tag like ('gift_mode_%')) -- find ref tags of non-core visits 
      or boe_referrer like ('boe_gift_mode%')) 
  -- and (ref_tag like ('gm_gift_idea_listings%') -- persona listing view, web
  -- or ref_tag like ('gm_occasion_gift_idea_listings-%') -- occasion listing view, web
  -- or ref_tag like ('gm_deluxe_persona_card%') -- quiz listing view, web
  -- or ref_tag like ('listing_suggested_persona_listings_related%') -- listings at bottom of listing page, web, NOT CORE 
  -- or boe_referrer like ('boe_gift_mode_popular_gift_listings%') -- popular gift ideas listing view, web
  -- or boe_referrer like ('boe_gift_mode_gift_idea_listings%')-- persona listing view, boe
  -- or boe_referrer like ('gift_mode_occasion%')-- occasion listing view, boe
  -- or boe_referrer like ('boe_gift_mode_editors_picks_listings%'))-- occasion listing view, boe NEED TO CONFIRM
)
, agg as (
select
	b._date  
	, b.platform 
  , b.browser_platform 
	, b.region  
  , b.is_admin_visit as admin 
  , b.top_channel 
  , a.listing_id
  , a.visit_id
  , coalesce(count(*),0) as n_listing_views
	, coalesce(max(c.purchased_after_view),0) as purchased_after_view
from 
  listing_views a
inner join 
  etsy-data-warehouse-prod.weblog.visits b
    using (_date, visit_id)
left join 
  `etsy-data-warehouse-prod`.analytics.listing_views c
    on a.listing_id=c.listing_id
    and a.visit_id=c.visit_id
    and a._date=c._date
    and a.sequence_number=a.sequence_number 
where 
  b._date >= current_date-2
)
select
	a._date
	, a.platform
	, a.region
	, a.is_admin_visit
	, a.top_channel
	, sum(a.n_listing_views) as total_listing_views
	, coalesce(sum(case when (a.ref_tag like ('%gift_mode_%') or a.ref_tag like ('gm_%')) then a.n_listing_views end),0) as core_listing_views
	, count(distinct a.listing_id) as unique_listings_viewed
	, count(distinct transaction_id) as unique_transactions
	, sum(a.purchased_after_view) as total_purchased_listings
	, coalesce(sum(b.trans_gms_net),0) as attr_gms
from agg a
left join listing_gms b
);

end
