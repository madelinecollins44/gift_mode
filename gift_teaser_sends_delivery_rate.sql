BEGIN


create or replace table `etsy-data-warehouse-dev.rollups.gift_teaser_email_rates` as ( 
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
	date(timestamp_seconds(create_date)) >= current_date-365 --gift teasers created in last year
	-- and date(timestamp_seconds(create_date)) < current_date-365 -- create before current_date
	and email_send_schedule_option != 2
	and gifting_token is not null
	and delete_date is null
	and recipient_email > ""
	and (date(timestamp_seconds(email_scheduled_send_date))) >= current_date-365 
)
, email_metrics as (
select distinct
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
	and a.email_sent_date = date(timestamp_seconds(b.send_date))
	and date(timestamp_seconds(b.send_date)) >= current_date-365
left join 
	`etsy-data-warehouse-prod.mail_mart.bounces` c
	on 
		a.recipient_email = lower(c.email_address) 
		and c.campaign_label like "recipient_%"
    and a.email_sent_date = date(timestamp_seconds(c.send_date))
		and date(timestamp_seconds(c.send_date)) >= current_date-365
left join 
  etsy-data-warehouse-prod.mail_mart.opens d
    on a.recipient_email = lower(d.email_address) 
	  and d.campaign_label like "recipient_%"
    and a.email_sent_date = date(timestamp_seconds(d.send_date))
	  and date(timestamp_seconds(d.send_date)) >= current_date-365
		and d.euid=b.euid
left join 
  etsy-data-warehouse-prod.mail_mart.clicks e
    on a.recipient_email = lower(e.email_address) 
	  and e.campaign_label like "recipient_%"
    and a.email_sent_date = date(timestamp_seconds(e.send_date))
	  and date(timestamp_seconds(e.send_date)) >= current_date-365
		and e.euid=d.euid 
)
select
  create_date
  , create_page_source
  , count(distinct receipt_id) as created_gift_teasers
  , count(distinct case when email_sent_date is not null then receipt_id end) as sent_gift_teasers
  , count(distinct delivered) as delivered_gift_teasers
  , count(distinct bounced) as bounced_gift_teasers
  , count(distinct opened) as opened_gift_teasers
  , count(distinct clicked) as clicked_gift_teasers
from 
  email_metrics
group by all
);

END
