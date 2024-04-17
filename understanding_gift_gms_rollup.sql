------------------------------------------------------------------------
RERUN GIFT GMS ROLLUP TO GET DATA BACK TO 2020
------------------------------------------------------------------------
BEGIN 
create or replace temporary table purchases as (
select
	date(r.creation_tsz) as date 
	, tv.visit_id
	, tv.platform_app as platform 
	, tv.canonical_region as region 
	, tv.top_channel
	, r.receipt_id
	, a.transaction_id 
	, v.title
	, a.listing_id
	, t.trans_gms_net 
	, a.is_gift 
	, t.is_gift_card
	, a.requested_gift_wrap
	, a.trans_gift_wrap_price
	, r.is_gift_message
	, r.prior_receipt_tsz
	, case when g.receipt_id is not null then 1 else 0 end as is_recipient_view
	, case when regexp_contains(title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
from 
	`etsy-data-warehouse-prod`.transaction_mart.all_receipts r 
join
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions a 
using(receipt_id)
left join 
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans t 
using(transaction_id)
inner join 
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv 
on 
	a.transaction_id = tv.transaction_id
left join 
	`etsy-data-warehouse-prod`.etsy_shard.gift_receipt_options g 
on 
	r.receipt_id = g.receipt_id
left join 
	`etsy-data-warehouse-prod.schlep_views.transactions_vw` v 
on 
	v.transaction_id = a.transaction_id
where 
	a.date >= current_date - 1460
);


create or replace temporary table gift_searches as (
SELECT
	distinct _date, visit_id
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE 
	_date >= current_date - 1460
and is_gift > 0
)
;

create or replace temporary table gift_card_purchasing as (
select 
	a.date 
	, a.receipt_id 
	, a.transaction_id 
	, coalesce(max(u.amount_start_usd),0) as amount_start_usd
from 
	purchases  a 
join 
	`etsy-data-warehouse-prod.etsy_payments.giftcards` p
on 
	a.transaction_id = p.purchase_transaction_id
left join 
	`etsy-data-warehouse-prod.rollups.giftcard_usage` u 
on 
	p.giftcard_id = u.giftcard_id
group by 1,2,3
)
;

create or replace temporary table almost_agg_metrics as (
select 
	a.date 
	, platform 
	, region 
	, visit_id
  	, receipt_id
  	, case when prior_receipt_tsz is null then 1 else 0 end as new_buyer
	--GMS metrics
	, sum(trans_gms_net) as total_gms 
	, sum(case when is_gift > 0 or gift_title > 0 or b.visit_id is not null then trans_gms_net end) as total_gift_gms 
	, sum(case when is_gift > 0 then trans_gms_net end) as marked_as_gift_gms 
	, sum(case when b.visit_id is not null then trans_gms_net end) as gift_search_gms 
	, sum(case when gift_title > 0 then trans_gms_net end) as gift_title_gms 
	, coalesce(sum(trans_gift_wrap_price),0) as gift_wrap_gms
	, coalesce(sum(c.amount_start_usd),0) as gift_card_purchase_value
	, sum(case when is_recipient_view > 0 then trans_gms_net end) as gift_recipient_view_gms
	--orders
	, count(distinct a.receipt_id) as all_orders
	, count(distinct case when is_gift > 0 or gift_title > 0 or b.visit_id is not null then a.receipt_id end) as all_gift_orders 
	, count(distinct case when is_gift > 0 then a.receipt_id end) as marked_as_gift_orders 
	, count(distinct case when b.visit_id is not null then a.receipt_id end) as gift_search_orders
	, count(distinct case when gift_title > 0 then a.receipt_id end) as gift_title_orders
	, count(distinct case when is_gift_message > 0 then a.receipt_id end) as gift_message_orders
	, count(distinct case when requested_gift_wrap > 0 then a.receipt_id end) as gift_wrap_orders
	, count(distinct case when is_gift_card > 0 then a.receipt_id end) as gift_card_orders
	, count(distinct case when is_recipient_view > 0 then a.receipt_id end) as gift_recipient_view_orders
	--transactions
	, count(distinct transaction_id) as all_transactions
	, count(distinct case when is_gift > 0 or gift_title > 0 or b.visit_id is not null then transaction_id end) as all_gift_transactions 
	, count(distinct case when is_gift > 0 then transaction_id end) as marked_as_gift_transactions 
	, count(distinct case when b.visit_id is not null then transaction_id end) as gift_search_transactions
	, count(distinct case when gift_title > 0 then transaction_id end) as gift_title_transactions
	, count(distinct case when is_gift_message > 0 then transaction_id end) as gift_message_transactions
	, count(distinct case when requested_gift_wrap > 0 then transaction_id end) as gift_wrap_transactions
from 
	purchases a
left join 
	gift_searches b
using(visit_id)
left join 
	gift_card_purchasing c
using(date, receipt_id, transaction_id)
group by 1,2,3,4,5,6
)
;

create or replace temporary table gift_cross_join as (
  select 
  a.*
  ,b.external_source_decay_all
  ,c.top_channel
  ,c.second_channel
  ,c.third_channel
  ,c.utm_medium
  ,c.utm_campaign
  from almost_agg_metrics a
  left join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` b
    on a.visit_id = b.buy_visit_id
    and a.receipt_id=b.receipt_id
  left join `etsy-data-warehouse-prod.buyatt_mart.visits` c
    on b.o_visit_id = c.visit_id    
  where cast(b.receipt_timestamp as date)>= current_date - 1460
        and cast(c._date as date)>= current_date - 1460
);


create or replace temporary table agg_metrics as (
select
  date 
	, platform 
	, region 
	, top_channel
	, second_channel
	, third_channel
	, utm_medium
	, utm_campaign
	, new_buyer
	--GMS metrics
	, sum(total_gms*external_source_decay_all) as total_gms 
	, sum(total_gift_gms*external_source_decay_all) as total_gift_gms 
	, sum(marked_as_gift_gms*external_source_decay_all) as marked_as_gift_gms 
	, sum(gift_search_gms*external_source_decay_all) as gift_search_gms 
	, sum(gift_title_gms*external_source_decay_all) as gift_title_gms 
	, sum(gift_wrap_gms*external_source_decay_all) as gift_wrap_gms
	, sum(gift_card_purchase_value*external_source_decay_all) as gift_card_purchase_value
	, sum(gift_recipient_view_gms*external_source_decay_all) as gift_recipient_view_gms
	--orders
	, sum(all_orders*external_source_decay_all) as all_orders
	, sum(all_gift_orders*external_source_decay_all) as all_gift_orders 
	, sum(marked_as_gift_orders*external_source_decay_all) as marked_as_gift_orders 
	, sum(gift_search_orders*external_source_decay_all) as gift_search_orders
	, sum(gift_title_orders*external_source_decay_all) as gift_title_orders
	, sum(gift_message_orders*external_source_decay_all) as gift_message_orders
	, sum(gift_wrap_orders*external_source_decay_all) as gift_wrap_orders
	, sum(gift_card_orders*external_source_decay_all) as gift_card_orders
	, sum(gift_recipient_view_orders*external_source_decay_all) as gift_recipient_view_orders
	--transactions
	, sum(all_transactions*external_source_decay_all) as all_transactions
	, sum(all_gift_transactions*external_source_decay_all) as all_gift_transactions 
	, sum(marked_as_gift_transactions*external_source_decay_all) as marked_as_gift_transactions 
	, sum(gift_search_transactions*external_source_decay_all) as gift_search_transactions
	, sum(gift_title_transactions*external_source_decay_all) as gift_title_transactions
	, sum(gift_message_transactions*external_source_decay_all) as gift_message_transactions
	, sum(gift_wrap_transactions*external_source_decay_all) as gift_wrap_transactions
  from gift_cross_join
  group by 1,2,3,4,5,6,7,8,9
);


-- YY metrics

create or replace table `etsy-data-warehouse-dev.madelinecollins.gift_gms_metrics` as (
with tmp as (
select 
	date 
	, platform 
	, region 
	, top_channel
	, second_channel
	, third_channel
	, utm_medium
	, utm_campaign
	, new_buyer 
	--GMS metrics
	, total_gms 
	, total_gift_gms 
	, marked_as_gift_gms 
	, gift_search_gms 
	, gift_title_gms 
	, gift_wrap_gms
	, gift_card_purchase_value
	, gift_recipient_view_gms
	--orders
	, all_orders
	, all_gift_orders 
	, marked_as_gift_orders 
	, gift_search_orders
	, gift_title_orders
	, gift_message_orders
	, gift_wrap_orders
	, gift_card_orders
	, gift_recipient_view_orders
	--transactions
	, all_transactions
	, all_gift_transactions 
	, marked_as_gift_transactions 
	, gift_search_transactions
	, gift_title_transactions
	, gift_message_transactions
	, gift_wrap_transactions
	, "a" as flag 
from 
	agg_metrics a
union all
select
	date_add(date, interval 52 WEEK) AS date
	, platform 
	, region 
	, top_channel
  	, second_channel
  	, third_channel
  	, utm_medium
  	, utm_campaign
	, new_buyer 
	--GMS metrics
	, total_gms 
	, total_gift_gms 
	, marked_as_gift_gms 
	, gift_search_gms 
	, gift_title_gms 
	, gift_wrap_gms
	, gift_card_purchase_value
	, gift_recipient_view_gms
	--orders
	, all_orders
	, all_gift_orders 
	, marked_as_gift_orders 
	, gift_search_orders
	, gift_title_orders
	, gift_message_orders
	, gift_wrap_orders
	, gift_card_orders
	, gift_recipient_view_orders
	--transactions
	, all_transactions
	, all_gift_transactions 
	, marked_as_gift_transactions 
	, gift_search_transactions
	, gift_title_transactions
	, gift_message_transactions
	, gift_wrap_transactions
	, "b" as flag 
from 
	agg_metrics b
WHERE date_add(b.date, interval 52 WEEK) <= current_date - 1
union all 
select
	date_add(date, interval 1 year) AS date
	, platform 
	, region 
	, top_channel
	, second_channel
	, third_channel
	, utm_medium
	, utm_campaign
	, new_buyer 
	--GMS metrics
	, total_gms 
	, total_gift_gms 
	, marked_as_gift_gms 
	, gift_search_gms 
	, gift_title_gms 
	, gift_wrap_gms
	, gift_card_purchase_value
	, gift_recipient_view_gms
	--orders
	, all_orders
	, all_gift_orders 
	, marked_as_gift_orders 
	, gift_search_orders
	, gift_title_orders
	, gift_message_orders
	, gift_wrap_orders
	, gift_card_orders
	, gift_recipient_view_orders
	--transactions
	, all_transactions
	, all_gift_transactions 
	, marked_as_gift_transactions 
	, gift_search_transactions
	, gift_title_transactions
	, gift_message_transactions
	, gift_wrap_transactions
	, "c" as flag 
from 
	agg_metrics b
WHERE date_add(b.date, interval 1 year) <= current_date - 1
)
select
	date 
	, platform 
	, region 
	, top_channel
	, second_channel
	, third_channel
	, utm_medium
	, utm_campaign
	, new_buyer
	--TY metrics
	, coalesce(sum(case when flag = "a" then total_gms end),0) as total_gms
	, coalesce(sum(case when flag = "a" then total_gift_gms end),0) as total_gift_gms 
	, coalesce(sum(case when flag = "a" then marked_as_gift_gms end),0) as marked_as_gift_gms 
	, coalesce(sum(case when flag = "a" then gift_search_gms end),0) as gift_search_gms 
	, coalesce(sum(case when flag = "a" then gift_title_gms end),0) as gift_title_gms
	, coalesce(sum(case when flag = "a" then gift_wrap_gms end),0) as gift_wrap_gms
	, coalesce(sum(case when flag = "a" then gift_card_purchase_value end),0) as gift_card_purchase_value
	, coalesce(sum(case when flag = "a" then gift_recipient_view_gms end),0) as gift_recipient_view_gms
	, coalesce(sum(case when flag = "a" then all_orders end),0) as all_orders 
	, coalesce(sum(case when flag = "a" then all_gift_orders end),0) as all_gift_orders
	, coalesce(sum(case when flag = "a" then marked_as_gift_orders end),0) as marked_as_gift_orders
	, coalesce(sum(case when flag = "a" then gift_search_orders end),0) as gift_search_orders
	, coalesce(sum(case when flag = "a" then gift_title_orders end),0) as gift_title_orders
	, coalesce(sum(case when flag = "a" then gift_message_orders end),0) as gift_message_orders 
	, coalesce(sum(case when flag = "a" then gift_wrap_orders end),0) as gift_wrap_orders
	, coalesce(sum(case when flag = "a" then gift_card_orders end),0) as gift_card_orders
	, coalesce(sum(case when flag = "a" then gift_recipient_view_orders end),0) as gift_recipient_view_orders
	, coalesce(sum(case when flag = "a" then all_transactions end),0) as all_transactions 
	, coalesce(sum(case when flag = "a" then all_gift_transactions end),0) as all_gift_transactions
	, coalesce(sum(case when flag = "a" then marked_as_gift_transactions end),0) as marked_as_gift_transactions
	, coalesce(sum(case when flag = "a" then gift_search_transactions end),0) as gift_search_transactions
	, coalesce(sum(case when flag = "a" then gift_title_transactions end),0) as gift_title_transactions
	, coalesce(sum(case when flag = "a" then gift_message_transactions end),0) as gift_message_transactions
	, coalesce(sum(case when flag = "a" then gift_wrap_transactions end),0) as gift_wrap_transactions
	--DLY Metrics 
	, coalesce(sum(case when flag = "b" then total_gms end),0) as total_gms_dly
	, coalesce(sum(case when flag = "b" then total_gift_gms end),0) as total_gift_gms_dly
	, coalesce(sum(case when flag = "b" then marked_as_gift_gms end),0) as marked_as_gift_gms_dly
	, coalesce(sum(case when flag = "b" then gift_search_gms end),0) as gift_search_gms_dly 
	, coalesce(sum(case when flag = "b" then gift_title_gms end),0) as gift_title_gms_dly
	, coalesce(sum(case when flag = "b" then gift_wrap_gms end),0) as gift_wrap_gms_dly
	, coalesce(sum(case when flag = "b" then gift_card_purchase_value end),0) as gift_card_purchase_value_dly
	, coalesce(sum(case when flag = "b" then gift_recipient_view_gms end),0) as gift_recipient_view_gms_dly
	, coalesce(sum(case when flag = "b" then all_orders end),0) as all_orders_dly
	, coalesce(sum(case when flag = "b" then all_gift_orders end),0) as all_gift_orders_dly
	, coalesce(sum(case when flag = "b" then marked_as_gift_orders end),0) as marked_as_gift_orders_dly
	, coalesce(sum(case when flag = "b" then gift_search_orders end),0) as gift_search_orders_dly
	, coalesce(sum(case when flag = "b" then gift_title_orders end),0) as gift_title_orders_dly
	, coalesce(sum(case when flag = "b" then gift_message_orders end),0) as gift_message_orders_dly 
	, coalesce(sum(case when flag = "b" then gift_wrap_orders end),0) as gift_wrap_orders_dly
	, coalesce(sum(case when flag = "b" then gift_recipient_view_orders end),0) as gift_recipient_view_orders_dly
	, coalesce(sum(case when flag = "b" then all_transactions end),0) as all_transactions_dly 
	, coalesce(sum(case when flag = "b" then gift_card_orders end),0) as gift_card_orders_dly
	, coalesce(sum(case when flag = "b" then all_gift_transactions end),0) as all_gift_transactions_dly
	, coalesce(sum(case when flag = "b" then marked_as_gift_transactions end),0) as marked_as_gift_transactions_dly
	, coalesce(sum(case when flag = "b" then gift_search_transactions end),0) as gift_search_transactions_dly
	, coalesce(sum(case when flag = "b" then gift_title_transactions end),0) as gift_title_transactions_dly
	, coalesce(sum(case when flag = "b" then gift_message_transactions end),0) as gift_message_transactions_dly
	, coalesce(sum(case when flag = "b" then gift_wrap_transactions end),0) as gift_wrap_transactions_dly
	--YY Metrics 
	, coalesce(sum(case when flag = "c" then total_gms end),0) as total_gms_yy
	, coalesce(sum(case when flag = "c" then total_gift_gms end),0) as total_gift_gms_yy
	, coalesce(sum(case when flag = "c" then marked_as_gift_gms end),0) as marked_as_gift_gms_yy
	, coalesce(sum(case when flag = "c" then gift_search_gms end),0) as gift_search_gms_yy 
	, coalesce(sum(case when flag = "c" then gift_title_gms end),0) as gift_title_gms_yy
	, coalesce(sum(case when flag = "c" then gift_wrap_gms end),0) as gift_wrap_gms_yy
	, coalesce(sum(case when flag = "c" then gift_card_purchase_value end),0) as gift_card_purchase_value_dyy
	, coalesce(sum(case when flag = "c" then gift_recipient_view_gms end),0) as gift_recipient_view_gms_dyy
	, coalesce(sum(case when flag = "c" then all_orders end),0) as all_orders_yy
	, coalesce(sum(case when flag = "c" then all_gift_orders end),0) as all_gift_orders_yy
	, coalesce(sum(case when flag = "c" then marked_as_gift_orders end),0) as marked_as_gift_orders_yy
	, coalesce(sum(case when flag = "c" then gift_search_orders end),0) as gift_search_orders_yy
	, coalesce(sum(case when flag = "c" then gift_title_orders end),0) as gift_title_orders_yy
	, coalesce(sum(case when flag = "c" then gift_message_orders end),0) as gift_message_orders_yy
	, coalesce(sum(case when flag = "c" then gift_wrap_orders end),0) as gift_wrap_orders_yy
	, coalesce(sum(case when flag = "c" then gift_card_orders end),0) as gift_card_orders_yy
	, coalesce(sum(case when flag = "c" then gift_recipient_view_orders end),0) as gift_recipient_view_orders_yy
	, coalesce(sum(case when flag = "c" then all_transactions end),0) as all_transactions_yy
	, coalesce(sum(case when flag = "c" then all_gift_transactions end),0) as all_gift_transactions_yy
	, coalesce(sum(case when flag = "c" then marked_as_gift_transactions end),0) as marked_as_gift_transactions_yy
	, coalesce(sum(case when flag = "c" then gift_search_transactions end),0) as gift_search_transactions_yy
	, coalesce(sum(case when flag = "c" then gift_title_transactions end),0) as gift_title_transactions_yy
	, coalesce(sum(case when flag = "c" then gift_message_transactions end),0) as gift_message_transactions_yy
	, coalesce(sum(case when flag = "c" then gift_wrap_transactions end),0) as gift_wrap_transactions_yy
from 
	tmp 
group by 1,2,3,4,5,6,7,8,9
)
;
END
------------------------------------------------------------------------
GET YoY DATA FROM GIFT GMS ROLLUP 
------------------------------------------------------------------------
select 
  date_trunc(date, year) as year
  -- yoy
	, ((sum(total_gms)-sum(total_gms_dly))/(nullif(sum(total_gms_dly),0)))*100   as total_gms_yoy
	, ((sum(total_gift_gms)-sum(total_gift_gms_dly))/(nullif(sum(total_gift_gms_dly),0)))*100 as total_gift_gms_yoy
	, ((sum(marked_as_gift_gms)-sum(marked_as_gift_gms_dly))/(nullif(sum(marked_as_gift_gms_dly),0)))*100 as marked_as_gift_gms  
	, ((sum(gift_search_gms)-sum(gift_search_gms_dly))/(nullif(sum(gift_search_gms_dly),0)))*100 as gift_search_gms   
	, ((sum(gift_title_gms)-sum(gift_title_gms_dly))/(nullif(sum(gift_title_gms_dly),0)))*100 as gift_title_gms  
	, ((sum(all_orders)-sum(all_orders_dly))/(nullif(sum(all_orders_dly),0)))*100 as all_orders   
	, ((sum(all_gift_orders)-sum(all_gift_orders_dly))/(nullif(sum(all_gift_orders_dly),0)))*100 as all_gift_orders  
	, ((sum(marked_as_gift_orders)-sum(marked_as_gift_orders_dly))/(nullif(sum(marked_as_gift_orders_dly),0)))*100 as marked_as_gift_orders  
	, ((sum(gift_search_orders)-sum(gift_search_orders_dly))/(nullif(sum(gift_search_orders_dly),0)))*100 as gift_search_orders 
	, ((sum(gift_title_orders)-sum(gift_title_orders_dly))/(nullif(sum(gift_title_orders_dly),0)))*100 as gift_title_orders  
	, ((sum(all_transactions)-sum(all_transactions_dly))/(nullif(sum(all_transactions_dly),0)))*100 as all_transactions   
	, ((sum(all_gift_transactions)-sum(all_gift_transactions_dly))/(nullif(sum(all_gift_transactions_dly),0)))*100 as all_gift_transactions 
	, ((sum(marked_as_gift_transactions)-sum(marked_as_gift_transactions_dly))/(nullif(sum(marked_as_gift_transactions_dly),0)))*100 as marked_as_gift_transactions  
	, ((sum(gift_search_transactions)-sum(gift_search_transactions_dly))/(nullif(sum(gift_search_transactions_dly),0)))*100 as gift_search_transactions  
	, ((sum(gift_title_transactions)-sum(gift_title_transactions_dly))/(nullif(sum(gift_title_transactions_dly),0)))*100 as gift_title_transactions 
from 
   `etsy-data-warehouse-dev.madelinecollins.gift_gms_metrics`
  where date >= '2020-01-01'
  group by 1 


------------------------------------------------------------------------
YoY METRICS FOR GIFT TITLE 
--visits, acvv, conversion rate
------------------------------------------------------------------------
 create or replace table  etsy-data-warehouse-dev.madelinecollins.gift_title_transaction_visits as (
 select
  extract(year from b.date) as year
  , b.visit_id
from 
  (select 
   transaction_id 
  from 
     etsy-data-warehouse-prod.transaction_mart.all_transactions a
   inner join 
    etsy-data-warehouse-prod.listing_mart.listing_titles b
       using (listing_id)
  where 
     date >= '2020-01-01'
    and regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト")) a
  inner join 
    etsy-data-warehouse-prod.transaction_mart.transactions_visits b
      using (transaction_id)
 );

create or replace table  etsy-data-warehouse-dev.madelinecollins.gift_title_views as (
select 
  extract(year from _date) as year
  , visit_id
  , max(case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end) as gift_title
from  
  etsy-data-warehouse-prod.analytics.listing_views 
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using (listing_id)
where 
  _date >= '2020-01-01'
group by 1,2
);

---YOY CHANGE CALCULATION


--for each year, get # of visits, acvv, conversion rate 
-- --get all visits + year that made transaction with 'gift' in listing title since 2020
--  with gift_title_visits as (
--  select
--   extract(year from b.date) as year
--   , b.visit_id
-- from 
--   (select 
--    transaction_id 
--   from 
--      etsy-data-warehouse-prod.transaction_mart.all_transactions a
--    inner join 
--     etsy-data-warehouse-prod.listing_mart.listing_titles b
--        using (listing_id)
--   where 
--      date >= '2020-01-01'
--     and regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト")) a
--   inner join 
--     etsy-data-warehouse-prod.transaction_mart.transactions_visits b
--       using (transaction_id)
--  )
-- --for each year, get # of visits, acvv, conversion rate 
with yearly_metrics as (
select
  extract(year from v._date) as year
  , count(distinct v.visit_id) as total_visits
  , count(distinct gtv.visit_id) as gift_title_visits 
  , sum(v.total_gms)/count(distinct case when v.converted=1 then v.visit_id end) as total_acvv
  , sum(case when gtv.visit_id is not null then v.total_gms end)/count(distinct case when v.converted=1 then gtv.visit_id end) as gift_title_acvv
  , count(distinct case when v.converted =1 then v.visit_id end)/ count(distinct views.visit_id) as total_conversion_rate
  , count(distinct case when gtv.visit_id is not null and v.converted=1 then v.visit_id end)/ count(distinct case when views.gift_title=1 then views.visit_id end) as gift_title_conversion_rate
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  etsy-data-warehouse-dev.madelinecollins.gift_title_transaction_visits gtv
  using (visit_id)
left join 
  etsy-data-warehouse-dev.madelinecollins.gift_title_views views
    on v.visit_id=views.visit_id
where 
  v._date >= '2020-01-01'
  --v._date between '2023-01-01' and '2023-04-09'
  --or v._date between '2024-01-01' and '2024-04-09'
group by 1
)
SELECT
  a.year AS current_year
  , a.gift_title_visits AS current_year_visits
  , b.gift_title_visits AS previous_year_visits
  , a.gift_title_acvv AS current_year_acvv
  , b.gift_title_acvv AS previous_year_acvv
  , a.gift_title_conversion_rate AS current_year_conversion_rate
  , b.gift_title_conversion_rate AS previous_year_conversion_rate
  , ((a.gift_title_visits - b.gift_title_visits) / b.gift_title_visits) * 100 AS yoy_growth_visits  
  , ((a.gift_title_acvv - b.gift_title_acvv) / b.gift_title_acvv) * 100 AS yoy_growth_acvv
  , ((a.gift_title_conversion_rate - b.gift_title_conversion_rate) / b.gift_title_conversion_rate) * 100 AS yoy_growth_conversion_rate
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
group by 1,2,3,4,5,6,7,8,9
ORDER BY
  a.year;

------------------------------------------------------------------------
LISTING IN GIFT TITLE VS ACTIVE LISTINGS YOY
------------------------------------------------------------------------
with titles as (
select 
extract(year from date(timestamp(v.creation_tsz))) as year
, v.listing_id
, case when regexp_contains(v.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
, case when regexp_contains(l.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title_now
from
  `etsy-data-warehouse-prod.schlep_views.transactions_vw` v
left join
  `etsy-data-warehouse-prod.etsy_shard.listings` l 
    on v.listing_id = l.listing_id
where date(timestamp(v.creation_tsz)) >= '2020-01-01'
   --date(timestamp(v.creation_tsz)) between '2023-01-01' and '2023-04-09' ---------USE THESE NEXT TWO LINES TO FIND YOY FOR 2024
   -- or  date(timestamp(v.creation_tsz)) between '2024-01-01' and '2024-04-09'
)
, yearly_metrics as (
select 
  year
  , count(distinct listing_id) as purchased_listings
  , count(distinct case when gift_title = 1 then listing_id end) as gift_title_transaction
  , count(distinct case when gift_title_now=1 then listing_id end) as gift_title_now
  , count(distinct case when gift_title = 0 and gift_title_now=1 then listing_id end) as gift_title_change
from 
  titles 
group by 1
)
select
  a.year AS current_year
  , a.purchased_listings AS current_year_purchased_listings
  , b.purchased_listings AS previous_year_purchased_listings
  , a.gift_title_transaction AS current_year_gift_title_transaction
  , b.gift_title_transaction AS previous_year_gift_title_transaction
  , a.gift_title_now AS current_year_gift_title_now
  , b.gift_title_now AS previous_year_gift_title_now
  , a.gift_title_change AS current_year_gift_title_change
  , b.gift_title_change AS previous_year_gift_title_change
  , ((a.purchased_listings - b.purchased_listings) / b.purchased_listings) * 100 AS yoy_purchased_listings
  , ((a.gift_title_transaction - b.gift_title_transaction) / b.gift_title_transaction) * 100 AS yoy_gift_title_transaction
  , ((a.gift_title_now - b.gift_title_now) / b.gift_title_now) * 100 AS yoy_gift_title_now
  , ((a.gift_title_change - b.gift_title_change) / b.gift_title_change) * 100 AS yoy_gift_title_change

FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
group by 1,2,3,4,5,6,7,8,9
ORDER BY
  a.year;

------------------------------------------------------------------------
GIFT QUERY VISITS YOY 
------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.gift_query_visits as (
select
  extract(year from _date) as year
 , qs.visit_id
  , max(case when qm.is_gift = 1 then 1 else 0 end) as gift_query
from 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
join 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm 
    USING (query)
where _date >= '2020-01-01'
group by 1,2
);
