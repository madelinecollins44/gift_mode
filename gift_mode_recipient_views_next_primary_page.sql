create table if not exists `etsy-data-warehouse-prod.rollups.gift_mode_recipient_views_next_primary_page`  (
_date DATE 
  , platform STRING
  , region STRING
  , top_channel STRING
  , admin INT64
  , receipt_type STRING
  , next_page STRING
  , pageviews INT64
  , unique_visits INT64
  , browser_platform string
);
-- create table to show next page after recipient_view
create or replace temporary table next_page_after_recipient_view as (
select
_date
, visit_id
, event_type
, sequence_number
, next_page
, next_page_seq
, ref_tag
from (
  select
    _date
    , visit_id
    , event_type
    , sequence_number
    , lead(event_type) over (partition by visit_id order by sequence_number) as next_page
    , lead(sequence_number) over (partition by visit_id order by sequence_number) as next_page_seq
    , ref_tag
    from 
      event_sequence
    where page_view=1)
where event_type in ('giftreceipt_view','gift_receipt','gift_recipientview_boe_view')
);

delete from `etsy-data-warehouse-prod.rollups.gift_mode_recipient_views_next_primary_page` where _date >= last_date;

insert into `etsy-data-warehouse-prod.rollups.gift_mode_recipient_views_next_primary_page` (
  select
  l._date
  , l.platform
  , case when l.region in ("US", "GB", "FR", "DE", "CA", "AU") then region else "RoW" end as region
  , l.top_channel
  , l.admin
  , case 
      when l.landing_event in ('giftreceipt_view','gift_recipientview_boe_view') then 'digital_receipt'
      when l.landing_event_url like ('%gift_packingslip%') then 'physical_receipt'
      else 'error'
      end as receipt_type 
  , case
    when n.next_page is not null then n.next_page
    else 'exit'
    end as next_page
  , count(l.visit_id) as pageviews
  , count(distinct l.visit_id) as unique_visits
  , l.browser_platform 
  from landings l
  inner join next_page_after_recipient_view n using (_date, visit_id)
  group by all
)
;
