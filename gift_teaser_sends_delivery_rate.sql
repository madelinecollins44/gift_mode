BEGIN

declare last_date date;

-- drop table if exists `etsy-data-warehouse-dev.rollups.gift_teaser_email_metrics`;

create table if not exists `etsy-data-warehouse-dev.rollups.gift_teaser_email_metrics` (
 email_sent_date DATE
 , create_page_source int64
  , gift_teasers int64
  , delivered_gift_teasers int64
  , bounced_gift_teasers int64
  , opened_gift_teasers int64
  , clicked_gift_teasers int64
  , delivered_rate float64
  , bounced_rate float64
  , opened_rate float64
  , clicked_rate float64
);

-- in case of day 1, backfill for 30 days
-- set last_date = (select max(_date) from `etsy-data-warehouse-dev.rollups.gift_teaser_email_metrics`);
-- if last_date is null then set last_date = (select min(_date)-1 from `etsy-data-warehouse-prod.etsy_shard.gift_receipt_options`);
-- end if;

set last_date = current_date - 5;

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
	date(timestamp_seconds(create_date)) >= last_date
	and date(timestamp_seconds(create_date)) < last_date
	and email_send_schedule_option != 2
	and gifting_token is not null
	and email_sent_date is not null
	and delete_date is null
	and recipient_email > ""
	and (email_scheduled_send_date is null or date(timestamp_seconds(email_scheduled_send_date)) < last_date) -- send dates that are null or have been sent 
)
select 
	a.*
	, b.euid as delivered
  , c.euid as bounced
  , d.euid as opened
  , e.euid as clicked
from
	gift_teasers a 
left join 
	`etsy-data-warehouse-prod.mail_mart.delivered` b
on 
	a.recipient_email = lower(b.email_address) 
	and b.campaign_label like "recipient_%"
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
left join 
  etsy-data-warehouse-prod.mail_mart.clicks e
    on a.recipient_email = lower(e.email_address) 
	  and e.campaign_label like "recipient_%"
	  and date(timestamp_seconds(e.send_date)) >= last_date
);

insert into `etsy-data-warehouse-dev.rollups.gift_teaser_email_metrics` (
select
  email_sent_date
  , create_page_source
  , count(distinct receipt_id) as gift_teasers
  , count(distinct case when delivered is not null then receipt_id end) as delivered_gift_teasers
  , count(distinct case when bounced is not null then receipt_id end) as bounced_gift_teasers
  , count(distinct case when opened is not null then receipt_id end) as opened_gift_teasers
  , count(distinct case when clicked is not null then receipt_id end) as clicked_gift_teasers
  , count(distinct case when delivered is not null then receipt_id end)/count(distinct receipt_id) as delivered_rate
  , count(distinct case when bounced is not null then receipt_id end)/count(distinct receipt_id) as bounced_rate
  , count(distinct case when delivered is not null then receipt_id end)/count(distinct receipt_id) as opened_rate
  , count(distinct case when clicked is not null then receipt_id end)/count(distinct receipt_id) as clicked_rate
from 
  agg
group by all
);

END
