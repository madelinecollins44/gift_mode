--------------------------------------------------------------------------------------------------------------------------------------------------------
I TOOK THE OLD RECEIPT DATA ROLL UP AND ADDED IN: VIDEO, AUDIO, CONTENT FLAG, SHARED GT
--------------------------------------------------------------------------------------------------------------------------------------------------------

-- owner: awaagner@etsy.com
-- owner_team: product-asf@etsy.com
-- description: tracking receipt email info between gift purchases

BEGIN 

-- create a table with info about the gift receipts, including info on whether they"ve been visited

create or replace table `etsy-data-warehouse-dev.rollups.gift_receipt_data` as ( 
with gifting_receipts as (
select
  date(timestamp_seconds(gr.create_date)) as create_date
  , gr.delete_date
  , gr.email_sent_date
  , gr.receipt_id
  , gr.buyer_first_name
  , gr.recipient_email
  , gr.gifting_token
  , gr.show_gift_item
  , gr.gifting_token_expiration_date
  , gr.show_tracking_info
  , gr.email_send_schedule_option
  , gr.cart_id
  , gr.create_page_source
  , gr.thank_you_note
  , med.media_id -- video=0, audio=1 
  , case when flag.gift_receipt_options_id is not null then 1 else 0 end as moderation_flag
from
  `etsy-data-warehouse-prod.etsy_shard.gift_receipt_options` gr
left join 
  (select gift_receipt_options_id, media_id from etsy-data-warehouse-prod.etsy_shard.gift_receipt_media where state != 2) med
    using (gift_receipt_options_id)
left join 
    (select 
        JSON_VALUE(reason, "$.gift_receipt_options_id") as gift_receipt_options_id 
      from `etsy-data-warehouse-prod.etsy_aux.flag` 
        where flag_type_id = 1262867763708)  flag
    on gr.gift_receipt_options_id=cast(flag.gift_receipt_options_id as int64)
where
  gr.receipt_id is not null
  and gr.delete_date is null
  and gr.gifting_token is not null
)
, all_gift_receipt_data as (
select
  a.* 
  , ar.buyer_user_id
  , up.primary_email as buyer_email
  , upd.mapped_user_id as recipient_user_id
  , date(timestamp_seconds(upd.join_date)) as recipient_join_date
  , ar.is_guest_checkout
from 
  gifting_receipts a
left join `etsy-data-warehouse-prod.transaction_mart.all_receipts` ar using (receipt_id)
left join 
  `etsy-data-warehouse-prod`.user_mart.mapped_user_profile up 
on ar.buyer_user_id = up.mapped_user_id
left join 
  `etsy-data-warehouse-prod`.user_mart.mapped_user_profile upd 
on 
  lower(a.recipient_email) = upd.primary_email
  and a.recipient_email > ""
)
, receipt_info as (
select
  a.date as receipt_date
  , b.receipt_id
  , coalesce(max(t.is_gift),0) as marked_as_gift
  , coalesce(sum(a.trans_gms_net),0) as trans_gms_net
  , coalesce(sum(a.trans_giftwrap_gms_net),0) as gift_wrap_gms_net
  , count(distinct a.transaction_id) as n_transactions
from 
  `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` a
join 
  `etsy-data-warehouse-prod`.transaction_mart.all_transactions t 
on 
  a.transaction_id = t.transaction_id
inner join 
  all_gift_receipt_data b 
on 
  a.receipt_id=b.receipt_id
group by 1,2
), visit_data as (
select 
  a.receipt_id
  , count(distinct visit_id) as total_visits
  , count(distinct case when user_id = buyer_user_id then visit_id end) as sender_visits
  , count(distinct case when user_id = recipient_user_id then visit_id end) as recipient_visits 
  , count(distinct case when user_id = 0 then visit_id end) as signed_out_visits
from 
    `etsy-data-warehouse-prod.rollups.gift_receipt_visits` a
left join 
  all_gift_receipt_data b 
using(receipt_id)
group by 1
)
select 
  a.create_date 
  , a.show_gift_item 
  , a.show_tracking_info 
  , a.email_send_schedule_option
  , case when buyer_email = recipient_email then 1 else 0 end as sent_to_self
  , a.create_page_source
  , a.is_guest_checkout
  , a.media_id
  , a.moderation_flag
  , case when a.thank_you_note is not null then 1 else 0 end as thank_you_note_sent
  , coalesce(count(distinct a.receipt_id),0) as n_orders
  , coalesce(sum(n_transactions),0) as n_transactions
  , coalesce(sum(trans_gms_net),0) as total_gift_receipt_order_gms
  , coalesce(sum(gift_wrap_gms_net),0) as total_gift_receipt_gift_wrap_gms
  , coalesce(count(distinct case when marked_as_gift > 0 then a.receipt_id end),0) as marked_as_gift_orders
  , coalesce(count(distinct case when email_sent_date is not null then a.receipt_id end),0) as email_sent_orders 
  , coalesce(count(distinct case when recipient_join_date is null or recipient_join_date >= b.receipt_date then a.receipt_id end),0) as new_buyer_recipient_orders
  , coalesce(count(distinct case when recipient_join_date < b.receipt_date then a.receipt_id end),0) as existing_buyer_recipient_orders
  , coalesce(count(distinct case when recipient_join_date >= b.receipt_date then recipient_email end),0) as new_buyers_acquired
  , count(distinct case when recipient_visits > 0 then a.receipt_id end) as receipts_with_recipient_visit 
  , count(distinct case when total_visits > 0 then a.receipt_id end) as receipts_with_any_visit
  , count(distinct case when sender_visits > 0 then a.receipt_id end) as receipts_with_sender_visit 
  , count(distinct case when signed_out_visits > 0 then a.receipt_id end) as receipts_with_signed_out_visit
  , sum(total_visits) as total_visits 
  , sum(sender_visits) as sender_visits 
  , sum(recipient_visits) as recipient_visits
  , sum(signed_out_visits) as signed_out_visits
from 
  all_gift_receipt_data a 
left join 
  receipt_info b 
    using(receipt_id)
left join 
  visit_data v 
on 
  a.receipt_id = v.receipt_id
group by all
)
;

END
