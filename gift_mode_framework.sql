---can i make a temp table with all necessary events and update there instead of inquery as more events are added?
begin

declare last_date date;

drop table if exists `etsy-data-warehouse-dev.rollups.gift_mode_visits_kpis`;

create table if not exists `etsy-data-warehouse-dev.rollups.gift_mode_visits_kpis` (
  	_date DATE
	, platform STRING
  , browser_platform STRING
	, region  STRING
  , admin int64
  , top_channel STRING
  , total_gm_visits int64
  , total_gm_impressions int64
  , core_gm_visits int64
  , core_gm_impressions int64
  , visits_with_gm_click int64
  , total_gm_clicks int64
  , unique_visits_with_purchase int64
  , unique_visits_with_core_purchase int64
  , total_listing_views int64
  , total_core_listing_views int64
  , listings_viewed int64
  , core_listings_viewed int64
  , visits_with_core_listing_view int64
  , total_purchased_listings int64
  , total_purchased_core_listings int64
 	, unique_transactions int64
	, attr_gms NUMERIC 
);

-- in case of day 1, backfill for 30 days
-- set last_date= (select max(_date) from `etsy-data-warehouse-prod.rollups.gift_mode_gift_idea_stats`);
--  if last_date is null then set last_date= (select min(_date)-1 from `etsy-data-warehouse-prod.weblog.events`);
--  end if;

set last_date= current_date - 1;


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
    date(_partitiontime) >= last_date
	  and (((beacon.event_name = 'recommendations_module_delivered' 
        and ((select value from unnest(beacon.properties.key_value) where key = 'module_placement') in ('lp_suggested_personas_related','homescreen_gift_mode_personas'))) --related personas module on listing page, web AND app home popular personas module delivered, boe
        or (select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('hub_stashgrid_module-%') --Featured personas on hub, web
            or (select value from unnest(beacon.properties.key_value) where key = 'module_placement') like ('hub_stashgrid_module-%')) --Featured personas on hub, web
    or (beacon.event_name like ('%gm_%') or beacon.event_name like ('%gift_mode%') or  beacon.event_name like ('market_gift_personas_%')))
)
select 
	b._date  
	, a.platform 
  , a.browser_platform 
	, a.region  
  , a.is_admin_visit as admin 
  , a.top_channel 
  , visit_id
  , count(visit_id) as impressions
  , max(case when event_name in ('gift_mode_home','gift_mode_persona','gift_mode_occasions_page','gift_mode_browse_all_personas','gift_mode_see_all_personas','gift_mode_results','gift_mode_quiz_results') then 1 else 0 end) as core_visit
    , count(case when event_name in ('gift_mode_home','gift_mode_persona','gift_mode_occasions_page','gift_mode_browse_all_personas','gift_mode_see_all_personas','gift_mode_results','gift_mode_quiz_results') then visit_id end) as core_impressions
from 
  etsy-data-warehouse-prod.weblog.visits a
inner join 
  get_recmods_events b 
    using (_date, visit_id)
where 
  a._date >= last_date
group by all
);

--this table looks at visits with gift_mode specific ref_tags		
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
  and (ref_tag like ('hp_promo_secondary_042224_US_Gifts_%') -- Onsite Promo Banner (Mother's Day/ Father's Day), web
      or ref_tag like ('hp_promo_tertiary_042224_US_Gifts_%') -- Onsite Promo Banner (Mother's Day/ Father's Day), web
      or ref_tag like ('gm%') -- mostly everything else 
      or ref_tag like ('%GiftMode%') --Gift Teaser promo banner on hub, web
      or ref_tag like ('hp_gm%') -- Shop by occasion on homepage, web
      or ref_tag like ('GiftTeaser%')) -- Skinny Banner (Mother's Day), web
)
select
	_date 
	, visit_id 
  , count(visit_id) as clicks
from get_refs
group by all 
);
    --banners + other ingresses
    --   'hp_gm_shop_by_occasion_module' -- Shop by occasion on homepage, web
	  --   , 'listing_suggested_personas_related' --Related personas module/ personas variant, web
	  --   , 'hub_GiftMode' --Gift Teaser promo banner on hub, web
    --   , 'GiftTeaser_MDAY24_Skinny_Sitewide' -- Skinny Banner (Mother's Day), web
    --   , 'gm_market_personas_query_related' --Related personas module on market page, web
    --   , 'gm_market_personas_popular'--popular personas module on market page, web
    --   , 'gm-hp-banner' -- hampage banner gift mode ingress clicked, web
    --   --ref tags on core pages
    --   , 'gm_popular_personas' --Popular gift ideas persona card clicked from gift mode home, web
    --   , 'gm_popular_gift_listings' --Popular gifts listing card clicked from gift mode home, 
    --   , 'gm-global-nav'-- Global nav item with text on web
    -- -- 'like' ref tags from banners + ingresses
	  --   or (ref_tag like ('hp_promo_secondary_042224_US_Gifts_%')-- Onsite Promo Banner (Mother's Day/ Father's Day), web
    --   or ref_tag like ('hp_promo_tertiary_042224_US_Gifts_%')-- Onsite Promo Banner (Mother's Day/ Father's Day), web
    --   or ref_tag like ('gm-hp-banner-persona%') -- persona card on homepage banner clicked, web

--this table looks at all gift mode related listing views + purchases 
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
	tv.date >= last_date
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
	_date >= last_date
	and event_type = "view_listing"
  and ((ref_tag like ('gm_%')) -- find ref tags of non-core visits 
      or referrer like ('boe_gift_mode%')) 
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
  , a.listing_id
  , a.visit_id
  , coalesce(count(*),0) as n_listing_views
  , max(case when a.ref_tag like ('gm%') then 1 else 0 end) as core_listing
	, max(c.purchased_after_view) as purchased_after_view
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
  b._date >= last_date
group by all
)
select
	a._date
  , a.visit_id
	, sum(n_listing_views) as total_listing_views
  , sum(case when core_listing > 0 then n_listing_views end) as total_core_listing_views
  , count(distinct a.listing_id) as listings_viewed
  , count(distinct case when core_listing>0 then a.listing_id end) as core_listings_viewed
  , max(case when core_listing > 0 then 1 else 0 end) as visit_with_core_listing_view
  , sum(a.purchased_after_view) as total_purchased_listings
  , sum(case when core_listing > 0 then a.purchased_after_view end) as total_purchased_core_listings
  , max(case when purchased_after_view > 0 then 1 else 0 end) as visit_with_purchase
  , max(case when core_listing > 0 and purchased_after_view > 0 then 1 else 0 end) as visit_with_core_purchase
 	, count(distinct transaction_id) as unique_transactions
	, coalesce(sum(b.trans_gms_net),0) as attr_gms
from agg a
left join listing_gms b
    on a._date = b._date
	  and a.visit_id = b.visit_id
	  and a.listing_id = b.listing_id
	  and a.purchased_after_view > 0
group by all 
);

------------------------------------
--all together 
------------------------------------
insert into `etsy-data-warehouse-dev.rollups.gift_mode_visits_kpis` (
select
	a._date  
	, a.platform 
  , a.browser_platform 
	, a.region  
  , a.admin 
  , a.top_channel 
  , count(distinct a.visit_id,0) as total_gm_visits
  , coalesce(sum(a.impressions),0) as total_gm_impressions
  , coalesce(sum(a.core_visits),0) as core_gm_visits
  , coalesce(sum(a.core_impressions),0) as core_gm_impressions
  , count(distinct b.visit_id) visits_with_gm_click
  , sum(b.clicks) as total_gm_clicks
  , coalesce(count(distinct case when visit_with_purchase>0 then c.visit_id end),0) as unique_visits_with_purchase
  , coalesce(count(distinct case when visit_with_core_purchase>0 then c.visit_id end),0) as unique_visits_with_core_purchase
  , coalesce(sum(c.total_listing_views),0) as total_listing_views
  , coalesce(sum(c.total_core_listing_views),0) as total_core_listing_views
  , coalesce(sum(c.listing_id),0) as listings_viewed
  , coalesce(sum(c.core_listings_viewed),0) as core_listings_viewed
  , coalesce(sum(visit_with_core_listing_view),0) as visits_with_core_listing_view
  , coalesce(sum(a.total_purchased_listings),0) as total_purchased_listings
  , coalesce(sum(total_purchased_core_listings),0) as total_purchased_core_listings
 	, coalesce(sum(unique_transactions),0) as unique_transactions
	, coalesce(sum(b.trans_gms_net),0) as attr_gms
from 
  visits a
left join 
  clicks b
    using (visit_id, _date)
left join 
  listing_views c
    on a._date=c._date
    and a.visit_id=c.visit_id
group by all
);

end
