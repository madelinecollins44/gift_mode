BEGIN

declare last_date date;

-- drop table if exists `etsy-data-warehouse-dev.rollups.gift_teaser_email_rates`;

create table if not exists `etsy-data-warehouse-dev.rollups.gift_teaser_email_rates` (
 email_sent_date DATE
 , create_page_source int64
  , gift_teasers int64
  , delivered_gift_teasers int64
  , bounced_gift_teasers int64
  , opened_gift_teasers int64
  , clicked_gift_teasers int64
  , visited_gift_teasers int64
  -- , delivered_rate float64
  -- , bounced_rate float64
  -- , opened_rate float64
  -- , clicked_rate float64
);

-- in case of day 1, backfill for 30 days
set last_date = (select max(email_sent_date) from `etsy-data-warehouse-dev.rollups.gift_teaser_email_rates`);
if last_date is null then set last_date = (select min(date(timestamp_seconds(email_sent_date))) from `etsy-data-warehouse-prod.etsy_shard.gift_receipt_options`);
end if;

-- set current_date-10 = current_date - 1;

create or replace temporary table agg as (
with gift_teasers as (
select
	date(timestamp_seconds(create_date)) as create_date 
	, receipt_id 
	, create_page_source
	, email_send_schedule_option
	, date(timestamp_seconds(email_sent_date)) as email_sent_date
	, lower(recipient_email) as recipient_email
from 
	`etsy-data-warehouse-prod.etsy_shard.gift_receipt_options`
where 
	date(timestamp_seconds(create_date)) >= current_date-90 --all gift teasers created in 90 days 
	and date(timestamp_seconds(create_date)) < current_date -- create before current_date
	and email_send_schedule_option != 2
	and gifting_token is not null
	and email_sent_date is not null -- has been sent 
	and delete_date is null
	and recipient_email > ""
	and (date(timestamp_seconds(email_scheduled_send_date))) > last_date 
)
, visits as (
select distinct
  date(_partitiontime) as _date 
  , (select value from unnest(beacon.properties.key_value) where key in ('receipt_id')) as receipt_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`  
where 
  date(_partitiontime) >= current_date-90
  and beacon.event_name in ('giftreceipt_view', 'gift_recipientview_boe_view')
)
select distinct
	a.*
	, b.euid as delivered
  , c.euid as bounced
  , d.euid as opened
  , e.euid as clicked
	, case when f.receipt_id is not null then f.receipt_id end as visited 
from
	gift_teasers a 
left join 
	`etsy-data-warehouse-prod.mail_mart.delivered` b
on 
	a.recipient_email = lower(b.email_address) 
	and b.campaign_label like "recipient_%"
	and a.email_sent_date = date(timestamp_seconds(b.send_date))
	and date(timestamp_seconds(b.send_date)) >= last_date
left join 
	`etsy-data-warehouse-prod.mail_mart.bounces` c
	on 
		a.recipient_email = lower(c.email_address) 
		and c.campaign_label like "recipient_%"
		and date(timestamp_seconds(c.send_date)) >= last_date
left join 
  etsy-data-warehouse-prod.mail_mart.opens d
    on a.recipient_email = lower(d.email_address) 
	  and d.campaign_label like "recipient_%"
	  and date(timestamp_seconds(d.send_date)) >= last_date
		and d.euid=b.euid
left join 
  etsy-data-warehouse-prod.mail_mart.clicks e
    on a.recipient_email = lower(e.email_address) 
	  and e.campaign_label like "recipient_%"
	  and date(timestamp_seconds(e.send_date)) >= last_date
		and e.euid=d.euid -- is this right-- only opened emails can be clicked?
left join 
	visits f
		on a.receipt_id=cast(f.receipt_id as int64)
);

insert into `etsy-data-warehouse-dev.rollups.gift_teaser_email_rates` (
select
  email_sent_date
  , create_page_source
  , count(distinct receipt_id) as gift_teasers
  , count(distinct case when delivered is not null then receipt_id end) as delivered_gift_teasers
  , count(distinct case when bounced is not null then receipt_id end) as bounced_gift_teasers
  , count(distinct case when opened is not null then receipt_id end) as opened_gift_teasers
  , count(distinct case when clicked is not null then receipt_id end) as clicked_gift_teasers
  , count(distinct case when visited is not null then receipt_id end) as visited_gift_teasers
  -- , count(distinct case when delivered is not null then receipt_id end)/count(distinct receipt_id) as delivered_rate
  -- , count(distinct case when bounced is not null then receipt_id end)/count(distinct receipt_id) as bounced_rate
  -- , count(distinct case when delivered is not null then receipt_id end)/count(distinct receipt_id) as opened_rate
  -- , count(distinct case when clicked is not null then receipt_id end)/count(distinct receipt_id) as clicked_rate
from 
  agg
group by all
);

END


------------------------------------------------------------
--CHECK TO SEE IF RECEIPT_IDS HAVE MULTIPLE CAMPAIGNS
------------------------------------------------------------
-- with gift_teasers as (
-- select
-- date(timestamp_seconds(create_date)) as create_date
-- , receipt_id
-- , create_page_source
-- , email_send_schedule_option
-- , date(timestamp_seconds(email_sent_date)) as email_sent_date
-- , lower(recipient_email) as recipient_email
-- from
-- `etsy-data-warehouse-prod.etsy_shard.gift_receipt_options`
-- where
-- date(timestamp_seconds(create_date)) >= current_date-90 --all gift teasers created in 90 days
-- and date(timestamp_seconds(create_date)) < current_date -- create before current_date
-- and email_send_schedule_option != 2
-- and gifting_token is not null
-- and email_sent_date is not null -- has been sent
-- and delete_date is null
-- and recipient_email > ""
-- and (date(timestamp_seconds(email_scheduled_send_date))) > current_date-30
-- )
-- , find_campaigns as (
-- select
-- create_date
-- , email_sent_date
-- , a.receipt_id
-- , b.campaign_label
-- , a.recipient_email
-- , a.email_send_schedule_option
-- , count(case when b.campaign_label like "recipient_%" then b.campaign_label end) as recipient_campaigns
-- , count(case when b.campaign_label = "recipient_gift_receipt_buyercopy_2024_email" then b.campaign_label end) as recipient_gift_receipt_buyercopy_2024_email
-- , count(case when b.campaign_label = "recipient_gift_receipt_specific_date" then b.campaign_label end) as recipient_gift_receipt_specific_date
-- , count(case when b.campaign_label = "recipient_gift_receipt_afterpurchase" then b.campaign_label end) as recipient_gift_receipt_afterpurchase
-- , count(case when b.campaign_label = "recipient_gift_receipt_aftershipped" then b.campaign_label end) as recipient_gift_receipt_aftershipped
-- from
-- gift_teasers a
-- left join
-- `etsy-data-warehouse-prod.mail_mart.delivered` b
-- on
-- a.recipient_email = lower(b.email_address)
-- and b.campaign_label like "recipient_%"
-- and date(timestamp_seconds(b.send_date)) = a.email_sent_date
-- and date(timestamp_seconds(b.send_date)) >= current_date-30
-- group by all
-- order by a.receipt_id desc
-- )
-- select
-- *
-- from
-- find_campaigns a
-- where
-- (CASE WHEN recipient_gift_receipt_specific_date > 0 THEN 1 ELSE 0 END +
-- CASE WHEN recipient_gift_receipt_afterpurchase > 0 THEN 1 ELSE 0 END +
-- CASE WHEN recipient_gift_receipt_aftershipped > 0 THEN 1 ELSE 0 END) >= 2
-- group by all
