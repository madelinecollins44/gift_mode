------------------------------------------------------------------------------------------------------------------------------------------------------------------
VISITS WITH GIFT TEASER LANDINGS
--answers where users are viewing gift teasers on within the last week 
------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.gift_receipt_landings as (
with raw_data as (
select
  _date
  , visit_id
  , platform
from 
  etsy-data-warehouse-prod.weblog.visits v
where _date>= current_date-21
and landing_event in ('gift_recipientview_boe_view','giftreceipt_view')
)
select 
  _date
  , platform 
  , count(distinct visit_id) as unique_visits
  , count(visit_id) as visits
from 
  raw_data
group by 1,2
)
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------
WHICH RECEIPTS ARE GETTING VISITS
--gift receipt data: create table that looks at listing + receipt attributes 
--answers: how often do buyers share receipt, what are qualities of shared receipts (specifically is_gift), which platform receipts are getting created on, who is receving receipts, who is creating receipts 
------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.gift_receipt_attributes as (
with buyer_segements as (
select 
visit_id
, mapped_user_id
, platform
, buyer_segment
from etsy-data-warehouse-prod.rollups.visits_w_segments
where _date>=current_date-21
), raw_data as (
select
tv.date as _date
  , t.date as purchase_date
  , date(timestamp_seconds(o.create_date)) as create_date
  , tv.platform_app
  , o.receipt_id
  , t.listing_id
  , g.score
  , t.is_gift
  , b.buyer_segment
  , o.recipient_email
  , case 
      when o.create_page_source=0 then 'Unassigned'
      when o.create_page_source=1 then 'Checkout'
      when o.create_page_source=2 then 'Web Post-Purchase'
      when o.create_page_source=3 then 'BOE Post-Purchase'
    end as create_page_source
  , case when e.email is null and o.recipient_email is not null then 1 else 0 end as new_recipient_email
  , case when o.delete_date is not null or o.email_send_schedule_option is null then 1 else 0 end as gt_canceled --gt clicked then unclicked 
  , case when o.delete_date is null and o.gifting_token is not null and (o.email_send_schedule_option!=2)  then 1 end as gt_email_sent
from 
  etsy-data-warehouse-prod.etsy_shard.gift_receipt_options o
left join
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
        on tv.receipt_id=o.receipt_id
left join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions t
    on tv.receipt_id=t.receipt_id
left join   
  etsy-data-warehouse-prod.knowledge_base.listing_giftiness_v3 g
      on t.listing_id=g.listing_id
left join  
  etsy-data-warehouse-prod.etsy_index.email_addresses e
    on o.recipient_email=e.email
left join 
  buyer_segements b
    on t.mapped_user_id=b.mapped_user_id
where 
  tv.date>= current_date-21  
  and date(timestamp_seconds(o.create_date))>= current_date-15 --extend date of tv + ev tables to find assocated visit_ids w all gift receipts (including post-purchase)  
  and date(timestamp_seconds(o.create_date))< current_date
  and tv.platform_app in ('boe', 'mobile_web','desktop')
  and o.receipt_id is not null 
)
select
create_date -- want to use day gift_receipt was created
  , purchase_date
  , platform_app
  , is_gift
  , buyer_segment
  , create_page_source
  , count(distinct receipt_id) total_gift_receipts
  , avg(score) as average_giftiness_score
  , coalesce(count(distinct case when new_recipient_email=1 then receipt_id end),0) as new_recipient_email
  , coalesce(count(distinct case when gt_canceled=1 then receipt_id end),0) as canceled_gift_teasers
  , coalesce(count(distinct case when gt_email_sent=1 then receipt_id end),0) as email_sent_gift_teasers
from 
  raw_data
group by 1,2,3,4,5,6
);

------------------------------------------------------
WHICH RECEIPTS ARE GETTING VISITS
--answers: are receipts created on boe getting less visits? 
--------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.gift_receipt_visits as (
with receipt_views as ( --visits to gift receipts within the last week 
select 
  date(_partitiontime) as _date 
  , vi.platform
  , v.visit_id
  , beacon.user_id
  , sequence_number
  -- , beacon.loc
  , (select value from unnest(beacon.properties.key_value) where key in ('browser_id')) as browser_id
  , (select value from unnest(beacon.properties.key_value) where key in ('receipt_id')) as receipt_id
  , lead(beacon.event_name) over(partition by v.visit_id order by sequence_number) as next_page
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` v 
left join 
  etsy-data-warehouse-prod.weblog.visits vi 
    on date(v._partitiontime) =vi._date
    and v.visit_id=vi.visit_id
where 
  date(_partitiontime) >= current_date-21 
  and vi._date>= current_date-21--visits to gift receipts in last 7 days 
  and (beacon.primary_event=true or beacon.event_name in ('giftreceipt_view', 'gift_recipientview_boe_view'))
-- ) , visit_gift_receipt as (
--   select
--     g._date 
--     , g.visit_id
--     , g.user_id
--     , g.browser_id
--     , g.receipt_id
--     , g.next_page
--   from
--     receipt_views g
-- )
), create_gift_receipt as ( --gift receipts created in last week 
  select
  date(timestamp_seconds(o.create_date)) as _date --date when gift teaser was created
    , tv.platform_app
    , case 
        when o.create_page_source=0 then 'Unassigned'
        when o.create_page_source=1 then 'Checkout'
        when o.create_page_source=2 then 'Web Post-Purchase'
        when o.create_page_source=3 then 'BOE Post-Purchase'
      end as create_page_source
    , tv.visit_id
    , tv.user_id
    , t.is_gift
    , cast(o.receipt_id as string) as receipt_id
    , v.browser_id
    , case when o.delete_date is null and o.gifting_token is not null and (o.email_send_schedule_option =0 or o.email_send_schedule_option =1)  then 1 end as gt_email_sent
  from 
    etsy-data-warehouse-prod.etsy_shard.gift_receipt_options o
  left join 
    etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
      on o.receipt_id=tv.receipt_id
  left join 
    etsy-data-warehouse-prod.transaction_mart.all_transactions t
      on tv.receipt_id=t.receipt_id
  left join 
    etsy-data-warehouse-prod.weblog.visits v
    on tv.visit_id=v.visit_id
    and tv.user_id=v.user_id
  where tv.date>= current_date-21 --went back 15 days to find visit_ids for post purchase gift teasers, but use create date as date these get created
  and date(timestamp_seconds(o.create_date))>= current_date-15 
  and date(timestamp_seconds(o.create_date))< current_date
  and v._date>= current_date-21
  ), listing_attributes as 
    (select
      l.listing_id
      , t.receipt_id
      , case when l.title like '%gift%' then 1 else 0 end as gift_title
    from
      etsy-data-warehouse-prod.listing_mart.listing_titles l
    inner join 
      etsy-data-warehouse-prod.transaction_mart.all_transactions t
        using (listing_id)
  )
  select
    c._date --date gift teaser was created, so gift teaser created in last 7 days 
    , c.platform_app
    , v.platform as visit_platform
    , c.create_page_source
    , c.is_gift
    , l.gift_title
    , c.gt_email_sent as shared_email
    , case 
        when v.next_page is null then 'exit'
        else v.next_page
      end as next_page
    , count(distinct c.receipt_id) as created_receipts
    , count(distinct case when v.receipt_id is not null then v.receipt_id end) as  visited_receipts
    , count(v.visit_id) as visits_to_receipt
    , count(distinct case when v.user_id != c.user_id then v.receipt_id end) as visit_diff_user
    , count(distinct case when v.user_id = c.user_id then v.receipt_id end) as visit_same_user
    , count(distinct case when v.browser_id != c.browser_id then v.receipt_id end) as visit_diff_browser
    , count(distinct case when v.browser_id = c.browser_id then v.receipt_id end) as visit_same_browser
  from 
    create_gift_receipt c
  left join 
    receipt_views v
      using (receipt_id)
  left join 
    listing_attributes l
      on c.receipt_id=cast(l.receipt_id as string)
  group by 1,2,3,4,5,6,7,8
  );

------------------------------------------------------
GIFT IN TITLE GIFT TEASERS
------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.gift_receipt_gift_title as (
with raw_data as (
select
tv.date as _date
  , date(timestamp_seconds(o.create_date)) as create_date
  , tv.platform_app
  , o.receipt_id
  , t.is_gift
  , o.recipient_email
  , case 
      when o.create_page_source=0 then 'Unassigned'
      when o.create_page_source=1 then 'Checkout'
      when o.create_page_source=2 then 'Web Post-Purchase'
      when o.create_page_source=3 then 'BOE Post-Purchase'
    end as create_page_source
  , case when o.delete_date is null and o.gifting_token is not null and (o.email_send_schedule_option =0 or o.email_send_schedule_option =1)  then 1 end as gt_email_sent
  , max(case when regexp_contains(title, "(?i)\bgift|\bcadeau|\bregalo|\bgeschenk|\bprezent|ギフト") then 1 else 0 end) as gift_title
from 
  etsy-data-warehouse-prod.etsy_shard.gift_receipt_options o
left join
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
        on tv.receipt_id=o.receipt_id
        -- and tv.date=date(timestamp_seconds(o.create_date)) -- want to match date of receipt CREATION with date of visit_id from receipt creation
left join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions t
    on tv.receipt_id=t.receipt_id
left join 
  etsy-data-warehouse-prod.listing_mart.listing_titles l
    on t.listing_id=l.listing_id
where 
  tv.date>= current_date-21  
  and date(timestamp_seconds(o.create_date))>= current_date-15 --extend date of tv + ev tables to find assocated visit_ids w all gift receipts (including post-purchase)  
  and date(timestamp_seconds(o.create_date))< current_date
  and tv.platform_app in ('boe', 'mobile_web','desktop')
  and o.receipt_id is not null 
group by 1,2,3,4,5,6,7,8
)
select
create_date -- want to use day gift_receipt was created
  , platform_app
  , is_gift
  , create_page_source
  , gift_title
  , count(distinct receipt_id) total_gift_receipts
  , coalesce(count(distinct case when gt_email_sent=1 then receipt_id end),0) as email_sent_gift_teasers
from 
  raw_data
group by 1,2,3,4,5
);

------------------------------------------------------
RECEIPIENTS BY BUYER SEGMENTS
------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.gift_receipt_shares as (
---get receipt_id, share success, then close
with receipt_actions as (
select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
    , lead(beacon.event_name) over (partition by visit_id order by sequence_number) as next_event
		, (select value from unnest(beacon.properties.key_value) where key = 'receipt_id') as receipt_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons` 
	where
		date(_partitiontime) >= current_date-21
	and
		beacon.event_name in ('boe_receipt_gift_link_create','gift_receipt_share_success','gift_recipientview_boe_buyer_close')
), agg as (
select 
distinct receipt_id
, count(case when event_name in ('boe_receipt_gift_link_create') then visit_id end) as creates
, count(distinct case when event_name in ('boe_receipt_gift_link_create') then visit_id end) as unique_creates
, count(case when event_name in ('boe_receipt_gift_link_create') 
        and next_event in ('gift_receipt_share_success') 
        then visit_id end) as shared_success
, count(case when event_name in ('boe_receipt_gift_link_create') 
        and next_event in ('gift_recipientview_boe_buyer_close') 
        then visit_id end) as not_shared_success
from 
  receipt_actions
group by 1
), web_link_shares as (
select distinct
  url
  , case 
      when REGEXP_EXTRACT(url, '/purchases/([^/?]+)') is not null then REGEXP_EXTRACT(url, '/purchases/([^/?]+)')
      when REGEXP_EXTRACT(url, '/purchases/([^/?]+)') is null then REGEXP_EXTRACT(url, r'receipt_id=(\d+)&') 
      else null
    end as receipt_id
  from etsy-data-warehouse-prod.weblog.events 
  where event_type in ('gift_sharelink_hasinputs_clicked')
  and _date>= current_date-21
) , web_agg as (
select 
  distinct cast(receipt_id as int64) as receipt_id
from 
  web_link_shares
where receipt_id != 'canceled'
), receipt_options as (
select
tv.date as _date
, o.receipt_id
, tv.platform_app as create_platform
, t.is_gift
  , case 
      when o.create_page_source=0 then 'Unassigned'
      when o.create_page_source=1 then 'Checkout'
      when o.create_page_source=2 then 'Web Post-Purchase'
      when o.create_page_source=3 then 'BOE Post-Purchase'
    end as create_page_source
  , case when o.delete_date is null and o.gifting_token is not null and (o.email_send_schedule_option!=2) then 1 else 0 end as gt_email_sent
  , max(case when regexp_contains(title, "(?i)\bgift|\bcadeau|\bregalo|\bgeschenk|\bprezent|ギフト") then 1 else 0 end) as gift_title
from 
  etsy-data-warehouse-prod.etsy_shard.gift_receipt_options o
left join
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
        on tv.receipt_id=o.receipt_id
left join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions t
    on tv.receipt_id=t.receipt_id
left join 
  etsy-data-warehouse-prod.listing_mart.listing_titles l
    on t.listing_id=l.listing_id
where 
  tv.date>= current_date-21  
  and date(timestamp_seconds(o.create_date))>= current_date-15 --extend date of tv + ev tables to find assocated visit_ids w all gift receipts (including post-purchase)  
  and date(timestamp_seconds(o.create_date))< current_date
  and tv.platform_app in ('boe', 'mobile_web','desktop')
  and o.receipt_id is not null 
group by 1,2,3,4,5,6
)
select
	o._date as gt_create_data
	, o.create_platform
  , o.create_page_source
  , o.is_gift
  , o.gift_title
  , coalesce(count(distinct o.receipt_id),0) as total_created_gt
  , count(distinct case when o.gt_email_sent=1 then o.receipt_id end) as email_sent
	, coalesce(count(distinct a.receipt_id),0) as total_creates_boe
	, coalesce(count(distinct case when a.shared_success>0 then a.receipt_id end)) as successful_shares_boe 
  , coalesce(count(distinct case when w.receipt_id is not null then w.receipt_id end),0) as web_link_shared
  , coalesce(count(distinct case when a.shared_success>0 or o.gt_email_sent>0 or w.receipt_id is not null then o.receipt_id end),0) as distinct_shared_receipts

from 
	receipt_options o
left join 
	agg a
		on o.receipt_id=cast(a.receipt_id as int64)
left join 
  web_agg w
    on o.receipt_id=cast(w.receipt_id as int64)
group by 1,2,3,4,5
);

--query to find share of total orders by buyer segemnt
with buyers as (
select 
  buyer_segment
  , count(distinct t.buyer_user_id) as buyers
  , count(distinct transaction_id) as orders
  , count(distinct receipt_id) as receipts
from 
  etsy-data-warehouse-prod.rollups.visits_w_segments v
inner join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions t
    on t.buyer_user_id=v.user_id
where 
  v._date>=current_date-21
  and t.date>=current_date-21
group by 1)
select count(distinct transaction_id) as orders from etsy-data-warehouse-prod.transaction_mart.all_transactions where date>=current_date-21

------------------------------------------------------
SHARES BY TIAG VS NOT TIAG
------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.gift_receipt_shares_visits as (
---get receipt_id, share success, then close
with receipt_actions as (
select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
    , lead(beacon.event_name) over (partition by visit_id order by sequence_number) as next_event
		, (select value from unnest(beacon.properties.key_value) where key = 'receipt_id') as receipt_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons` 
	where
		date(_partitiontime) >= current_date-21
	and
		beacon.event_name in ('boe_receipt_gift_link_create','gift_receipt_share_success','gift_recipientview_boe_buyer_close')
), agg as (
select 
distinct receipt_id
, count(case when event_name in ('boe_receipt_gift_link_create') then visit_id end) as creates
, count(distinct case when event_name in ('boe_receipt_gift_link_create') then visit_id end) as unique_creates
, count(case when event_name in ('boe_receipt_gift_link_create') 
        and next_event in ('gift_receipt_share_success') 
        then visit_id end) as shared_success
, count(case when event_name in ('boe_receipt_gift_link_create') 
        and next_event in ('gift_recipientview_boe_buyer_close') 
        then visit_id end) as not_shared_success
from 
  receipt_actions
group by 1
), web_link_shares as (
select distinct
  url
  , case 
      when REGEXP_EXTRACT(url, '/purchases/([^/?]+)') is not null then REGEXP_EXTRACT(url, '/purchases/([^/?]+)')
      when REGEXP_EXTRACT(url, '/purchases/([^/?]+)') is null then REGEXP_EXTRACT(url, r'receipt_id=(\d+)&') 
      else null
    end as receipt_id
  from etsy-data-warehouse-prod.weblog.events 
  where event_type in ('gift_sharelink_hasinputs_clicked')
  and _date>= current_date-21
) , web_agg as (
select 
  distinct cast(receipt_id as int64) as receipt_id
from 
  web_link_shares
where receipt_id != 'canceled'
), receipt_options as (
select
tv.date as _date
, o.receipt_id
, tv.platform_app as create_platform
, t.is_gift
  , case 
      when o.create_page_source=0 then 'Unassigned'
      when o.create_page_source=1 then 'Checkout'
      when o.create_page_source=2 then 'Web Post-Purchase'
      when o.create_page_source=3 then 'BOE Post-Purchase'
    end as create_page_source
  , case when o.delete_date is null and o.gifting_token is not null and (o.email_send_schedule_option!=2) then 1 else 0 end as gt_email_sent
  , max(case when regexp_contains(title, "(?i)\bgift|\bcadeau|\bregalo|\bgeschenk|\bprezent|ギフト") then 1 else 0 end) as gift_title
from 
  etsy-data-warehouse-prod.etsy_shard.gift_receipt_options o
left join
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
        on tv.receipt_id=o.receipt_id
left join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions t
    on tv.receipt_id=t.receipt_id
left join 
  etsy-data-warehouse-prod.listing_mart.listing_titles l
    on t.listing_id=l.listing_id
where 
  tv.date>= current_date-21  
  and date(timestamp_seconds(o.create_date))>= current_date-15 --extend date of tv + ev tables to find assocated visit_ids w all gift receipts (including post-purchase)  
  and date(timestamp_seconds(o.create_date))< current_date
  and tv.platform_app in ('boe', 'mobile_web','desktop')
  and o.receipt_id is not null 
group by 1,2,3,4,5,6
), shares as (
select
	o._date as gt_create_data
  , o.is_gift
  , o.receipt_id
  , case when a.shared_success>0 or o.gt_email_sent>0 or w.receipt_id is not null then 1 end as shared
from 
	receipt_options o
left join 
	agg a
		on o.receipt_id=cast(a.receipt_id as int64)
left join 
  web_agg w
    on o.receipt_id=cast(w.receipt_id as int64)
), visits as (
select 
  date(_partitiontime) as _date 
  , (select value from unnest(beacon.properties.key_value) where key in ('receipt_id')) as receipt_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`  
where 
  date(_partitiontime) >= current_date-21 
  and beacon.event_name in ('giftreceipt_view', 'gift_recipientview_boe_view')
)
select
gt_create_data
, is_gift
, count(distinct s.receipt_id) as shared_receipts
, count(distinct case when v.receipt_id is not null then s.receipt_id end) as visited_receipts
, count(distinct case when v.receipt_id is not null then 1 end) as visited_receipts_2
, count(case when v.receipt_id is null then 1 end) as not_visited_receipts
from 
  shares s
left join 
  visits v
    on s.receipt_id=cast(v.receipt_id as int64)
where s.shared=1
group by 1,2
);

------------------------------------------------------
VISITS BY SOURCE
------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.gift_receipt_shares_visits_source as (
---get receipt_id, share success, then close
with receipt_actions as (
select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
    , lead(beacon.event_name) over (partition by visit_id order by sequence_number) as next_event
		, (select value from unnest(beacon.properties.key_value) where key = 'receipt_id') as receipt_id
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons` 
	where
		date(_partitiontime) >= current_date-21
	and
		beacon.event_name in ('boe_receipt_gift_link_create','gift_receipt_share_success','gift_recipientview_boe_buyer_close')
), agg as (
select 
distinct receipt_id
, count(case when event_name in ('boe_receipt_gift_link_create') then visit_id end) as creates
, count(distinct case when event_name in ('boe_receipt_gift_link_create') then visit_id end) as unique_creates
, count(case when event_name in ('boe_receipt_gift_link_create') 
        and next_event in ('gift_receipt_share_success') 
        then visit_id end) as shared_success
, count(case when event_name in ('boe_receipt_gift_link_create') 
        and next_event in ('gift_recipientview_boe_buyer_close') 
        then visit_id end) as not_shared_success
from 
  receipt_actions
group by 1
), web_link_shares as (
select distinct
  url
  , case 
      when REGEXP_EXTRACT(url, '/purchases/([^/?]+)') is not null then REGEXP_EXTRACT(url, '/purchases/([^/?]+)')
      when REGEXP_EXTRACT(url, '/purchases/([^/?]+)') is null then REGEXP_EXTRACT(url, r'receipt_id=(\d+)&') 
      else null
    end as receipt_id
  from etsy-data-warehouse-prod.weblog.events 
  where event_type in ('gift_sharelink_hasinputs_clicked')
  and _date>= current_date-21
) , web_agg as (
select 
  distinct cast(receipt_id as int64) as receipt_id
from 
  web_link_shares
where receipt_id != 'canceled'
), receipt_options as (
select
tv.date as _date
, o.receipt_id
, tv.platform_app as create_platform
, t.is_gift
  , case 
      when o.create_page_source=0 then 'Unassigned'
      when o.create_page_source=1 then 'Checkout'
      when o.create_page_source=2 then 'Web Post-Purchase'
      when o.create_page_source=3 then 'BOE Post-Purchase'
    end as create_page_source
  , case when o.delete_date is null and o.gifting_token is not null and (o.email_send_schedule_option!=2) then 1 else 0 end as gt_email_sent
  , max(case when regexp_contains(title, "(?i)\bgift|\bcadeau|\bregalo|\bgeschenk|\bprezent|ギフト") then 1 else 0 end) as gift_title
from 
  etsy-data-warehouse-prod.etsy_shard.gift_receipt_options o
left join
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
        on tv.receipt_id=o.receipt_id
left join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions t
    on tv.receipt_id=t.receipt_id
left join 
  etsy-data-warehouse-prod.listing_mart.listing_titles l
    on t.listing_id=l.listing_id
where 
  tv.date>= current_date-21  
  and date(timestamp_seconds(o.create_date))>= current_date-15 --extend date of tv + ev tables to find assocated visit_ids w all gift receipts (including post-purchase)  
  and date(timestamp_seconds(o.create_date))< current_date
  and tv.platform_app in ('boe', 'mobile_web','desktop')
  and o.receipt_id is not null 
group by 1,2,3,4,5,6
), shares as (
select
	o._date as gt_create_data
  , o.create_page_source
  , o.receipt_id
  , case when a.shared_success>0 or o.gt_email_sent>0 or w.receipt_id is not null then 1 end as shared
from 
	receipt_options o
left join 
	agg a
		on o.receipt_id=cast(a.receipt_id as int64)
left join 
  web_agg w
    on o.receipt_id=cast(w.receipt_id as int64)
), visits as (
select distinct
  date(_partitiontime) as _date 
  , (select value from unnest(beacon.properties.key_value) where key in ('receipt_id')) as receipt_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`  
where 
  date(_partitiontime) >= current_date-21 
  and beacon.event_name in ('giftreceipt_view', 'gift_recipientview_boe_view')
)
select
gt_create_data
, create_page_source
, count(distinct s.receipt_id) as shared_receipts
, count(distinct case when v.receipt_id is not null then s.receipt_id end) as visited_receipts
, count(distinct case when v.receipt_id is not null then 1 end) as visited_receipts_2
, count(case when v.receipt_id is null then 1 end) as not_visited_receipts
from 
  shares s
left join 
  visits v
    on s.receipt_id=cast(v.receipt_id as int64)
where s.shared=1
group by 1,2
);


------------------------------------------------------
RECEIPIENTS BY BUYER SEGMENTS-- LAPSED BUYERS
------------------------------------------------------
-- with lapsed_buyers as ( --get buyer type of recipients: not made purchase in last 365 days 
-- select
--   a.user_id
--   , e.email
--   , max(a._date)
-- from  
-- 	etsy-data-warehouse-dev.madelinecollins.purchases a
-- left join 
--   etsy-data-warehouse-prod.etsy_index.email_addresses e
--     using (user_id)
-- group by 1,2
-- having max(a._date) < current_date-365 and max(a._date) > current_date-730 --most recent purchase was 
-- )
create or replace table etsy-data-warehouse-dev.madelinecollins.giftteaser_lapsedrecipients as (
with lapsed_buyers as (
select
  ea.email
  , ea.user_id
from 
  etsy-data-warehouse-prod.rollups.buyer_basics bb
inner join
  etsy-data-warehouse-prod.user_mart.user_mapping um
    on bb.mapped_user_id=um.mapped_user_id 
inner join 
  etsy-data-warehouse-prod.etsy_index.email_addresses ea
    on um.user_id=ea.user_id
where bb.buyer_segment in ('Lapsed')
)
, gift_receipts as ( --get recipient emails 
select
    o.receipt_id
  , tv.user_id
  , tv.visit_id
  , o.recipient_email 
  , date(timestamp_seconds(o.create_date)) as _date
from 
  etsy-data-warehouse-prod.etsy_shard.gift_receipt_options o
left join
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
    on tv.receipt_id=o.receipt_id
where -- all attributes that make more likely this was a sent gift teaser 
   delete_date is null  
   and gifting_token is not null 
   and email_send_schedule_option!=2
   and date(timestamp_seconds(create_date)) >= current_date-60
)
select
  count(distinct receipt_id) as receipts_sent
  , count(distinct case when lb.email is not null then lb.email end) as lapsed_recipients
  , count(distinct case when lb.email is not null then gr.receipt_id end) as receipts_to_lapsed
  , count(distinct lb.email) as lapsed_emails
  , count(distinct gr.recipient_email) as unique_receipt_emails
from 
  gift_receipts gr
left join 
  lapsed_buyers lb 
    on gr.recipient_email=lb.email
where _date >= current_date-15
);

------------------------------------------------------
RECEIPIENTS BY BUYER SEGMENTS
------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.giftteaser_buyer_segment_visits as (
with gift_receipts as ( --get recipient emails + user_id 
select
    o.receipt_id
  -- , tv.user_id
  , o._date
  , vi.browser_id
from 
  etsy-data-warehouse-dev.madelinecollins.receipts_last_30_days o
left join
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
    on tv.receipt_id=o.receipt_id
left join 
  etsy-data-warehouse-dev.madelinecollins.visits_last_30_days vi
    on tv.visit_id=vi.visit_id
left join   
  etsy-data-warehouse-dev.madelinecollins.buyer_segment_emails up
     on o.recipient_email=up.email
  where o._date >= current_date-30
), receipt_visits as (
select 
  v._date 
   , vi.browser_id
  -- , sequence_number
  , v.receipt_id
  , vs.buyer_segment
from 
  etsy-data-warehouse-dev.madelinecollins.receipts_visits_last_30_days v 
left join 
  etsy-data-warehouse-dev.madelinecollins.visits_last_30_days vi
    on v.visit_id=vi.visit_id
left join 
  etsy-data-warehouse-prod.rollups.visits_w_segments vs
    on v.visit_id=vs.visit_id
  where v._date >= current_date-30 and vs._date>= current_date-30
)
select
rc.buyer_segment
, count(distinct rc.receipt_id) as visited_receipts 
from 
  gift_receipts gr
left join
  receipt_visits rc 
  on gr.receipt_id=cast(rc.receipt_id as int64)
where gr.browser_id != rc.browser_id -- not sent to self 
group by 1
);

create or replace table etsy-data-warehouse-dev.madelinecollins.giftteaser_buyersegment_recipients as (
with gift_receipts as ( --get recipient emails + user_id 
select
    o.receipt_id
  , tv.user_id
  , date(timestamp_seconds(o.create_date)) as _date
  , o.recipient_email
  , vi.browser_id
  , up.buyer_segment
from 
  etsy-data-warehouse-prod.etsy_shard.gift_receipt_options o
left join
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
    on tv.receipt_id=o.receipt_id
left join 
  etsy-data-warehouse-prod.weblog.visits vi
    on tv.visit_id=vi.visit_id
left join   
  etsy-data-warehouse-prod.user_mart.mapped_user_profile up
     on o.recipient_email=up.primary_email
where -- all attributes that make more likely this was a sent gift teaser 
   delete_date is null  
   and gifting_token is not null 
   and email_send_schedule_option!=2
   and date(timestamp_seconds(create_date)) >= current_date-30
   and vi._date >= current_date-30
), receipt_visits as (
select 
  date(_partitiontime) as _date 
   , vi.browser_id
  -- , sequence_number
  , (select value from unnest(beacon.properties.key_value) where key in ('receipt_id')) as receipt_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` v 
left join 
  etsy-data-warehouse-prod.weblog.visits vi
    on v.visit_id=vi.visit_id
where 
  date(_partitiontime) >= current_date-30
  and vi._date >= current_date-30
  and (beacon.event_name in ('giftreceipt_view', 'gift_recipientview_boe_view'))
)
select
gr.buyer_segment
, count(distinct gr.receipt_id) as gift_receipts_sent
from 
  gift_receipts gr
left join
  receipt_visits rc 
  on gr.receipt_id=cast(rc.receipt_id as int64)
where gr.browser_id != rc.browser_id -- not sent to self 
group by 1
);

-- create or replace table etsy-data-warehouse-dev.madelinecollins.giftteaser_buyersegment_recipients as (
create or replace table etsy-data-warehouse-dev.madelinecollins.visits_last_30_days as 
(select
visit_id
, browser_id
from 
  etsy-data-warehouse-prod.weblog.visits 
where
 _date >= current_date-30
);

create or replace table etsy-data-warehouse-dev.madelinecollins.receipts_last_30_days as 
(select
    o.receipt_id
  -- , tv.user_id
  , date(timestamp_seconds(o.create_date)) as _date
  , o.recipient_email
from 
  etsy-data-warehouse-prod.etsy_shard.gift_receipt_options o
where -- all attributes that make more likely this was a sent gift teaser 
   delete_date is null  
   and gifting_token is not null 
   and email_send_schedule_option!=2
   and date(timestamp_seconds(create_date)) >= current_date-30
);

create or replace table etsy-data-warehouse-dev.madelinecollins.receipts_visits_last_30_days as 
(select 
  date(_partitiontime) as _date 
  , visit_id
  , (select value from unnest(beacon.properties.key_value) where key in ('receipt_id')) as receipt_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` v 
where 
  date(_partitiontime) >= current_date-30
  and (beacon.event_name in ('giftreceipt_view', 'gift_recipientview_boe_view'))
);

create or replace table etsy-data-warehouse-dev.madelinecollins.giftteaser_buyersegment_recipients as (
with gift_receipts as ( --get recipient emails + user_id 
select
    o.receipt_id
  -- , tv.user_id
  , o._date
  , o.recipient_email
  , vi.browser_id
  , up.buyer_segment
from 
  etsy-data-warehouse-dev.madelinecollins.receipts_last_30_days o
left join
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
    on tv.receipt_id=o.receipt_id
left join 
  etsy-data-warehouse-dev.madelinecollins.visits_last_30_days vi
    on tv.visit_id=vi.visit_id
left join   
  etsy-data-warehouse-prod.user_mart.mapped_user_profile up
     on o.recipient_email=up.primary_email
  where o._date >= current_date-15
), receipt_visits as (
select 
  v._date 
   , vi.browser_id
  -- , sequence_number
  , v.receipt_id
from 
  etsy-data-warehouse-dev.madelinecollins.receipts_visits_last_30_days v 
left join 
  etsy-data-warehouse-dev.madelinecollins.visits_last_30_days vi
    on v.visit_id=vi.visit_id
  where _date >= current_date-15
)
select
gr.buyer_segment
, count(distinct gr.receipt_id) as gift_receipts_sent
from 
  gift_receipts gr
left join
  receipt_visits rc 
  on gr.receipt_id=cast(rc.receipt_id as int64)
where gr.browser_id != rc.browser_id -- not sent to self 
group by 1
);
