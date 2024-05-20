with tmp as (
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
	date(timestamp_seconds(create_date)) >= current_date - 60
	and date(timestamp_seconds(create_date)) < current_date - 30	
	and email_send_schedule_option != 2
	and gifting_token is not null
	and email_sent_date is not null
	and delete_date is null
	and recipient_email > ""
	and (email_scheduled_send_date is null or date(timestamp_seconds(email_scheduled_send_date)) < current_date) -- send dates that are null or have been sent 
), email_sends as (
select 
	a.*
	, coalesce(b.euid,c.euid) as euid
from
	tmp a 
left join 
	`etsy-data-warehouse-prod.mail_mart.delivered` b
on 
	a.recipient_email = lower(b.email_address) 
	and b.campaign_label like "recipient_%"
	and date(timestamp_seconds(b.send_date)) >= current_date - 60
left join 
	`etsy-data-warehouse-prod.mail_mart.bounces` c
on 
	a.recipient_email = lower(c.email_address) 
	and c.campaign_label like "recipient_%"
	and date(timestamp_seconds(c.send_date)) >= current_date - 60

)
-- select * from email_sends where euid is null and email_sent_date < current_date
-- order by email_sent_date
-- limit 10;
select 
	-- create_date 
	-- , email_send_schedule_option
	count(distinct receipt_id) as gts_scheduled
	, count(distinct euid) as emails_sent 
	, count(distinct case when euid is not null then receipt_id end)/count(distinct receipt_id) as pct_w_email 
from 
	email_sends
-- group by all
-- order by 1
;
