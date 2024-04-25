--------------------------------------------------------
SHARES
--------------------------------------------------------

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

--------------------------------------------------------
VISITS
--------------------------------------------------------
----share rate of tiag vs non tiag orders
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
