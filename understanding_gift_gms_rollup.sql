------------------------------------------------------------------------
SITEWIDE YOY METRICS 
------------------------------------------------------------------------
with yearly_metrics as 
(select
  extract(year from v._date) as year
  , count(distinct v.visit_id) as visits
,  count(distinct case when v.converted =1 then v.visit_id end) as converted_visits
   , sum(gms.trans_gms_net)/count(distinct case when v.converted=1 then v.visit_id end) as total_acvv
  , count(distinct case when v.converted=1 then v.visit_id end)/ count(distinct v.visit_id) as conversion_rate
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
    on v.visit_id=tv.visit_id
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans gms
    on tv.transaction_id=gms.transaction_id
where  
  -- v._date >= '2020-01-01'
  (v._date between '2023-01-01' and '2023-04-09'
  or v._date between '2024-01-01' and '2024-04-09')
group by 1
)
select
 a.year AS current_year
  , a.visits as current_year_visits
  , b.visits as previous_year_visits
  , a.total_acvv AS current_year_total_acvv
  , b.total_acvv AS previous_year_total_acvv
  , a.conversion_rate AS current_year_conversion_rate
  , b.conversion_rate AS previous_year_conversion_rate
, a.converted_visits as current_year_converted_visits
  , b.converted_visits as previous_year_converted_visits
  , ((a.converted_visits - b.converted_visits) / nullif((b.converted_visits),0)) * 100 AS yoy_growth_converted_visits
  , ((a.visits - b.visits) / b.visits) * 100 AS yoy_growth_visits  
  , ((a.total_acvv - b.total_acvv) / nullif(b.total_acvv,0)) * 100 AS yoy_growth_acvv
  , ((a.conversion_rate - b.conversion_rate) / nullif((b.conversion_rate),0)) * 100 AS yoy_growth_conversion_rate
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY
  a.year;
	
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
LISTING ATTRIBUTES 
------------------------------------------------------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.active_listings_title as (
--   select
--   a.date
--   , case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
--   , a.listing_id
-- from 
--   etsy-data-warehouse-prod.incrementals.listing_daily a
-- inner join 
--   etsy-data-warehouse-prod.listing_mart.listing_titles b
--     using(listing_id)
-- where 
--   (a.date between '2024-01-01' and '2024-04-09') or 
--   (a.date between date('2023-01-01') and date('2023-04-09'))
-- group by all
-- );

select 
 a.gift_title
  , b.top_category
  , b.is_personalizable
  , b.is_digital
  , d.seller_tier
  , count(distinct a.listing_id)
from 
  etsy-data-warehouse-dev.madelinecollins.active_listings_title a
left join 
  `etsy-data-warehouse-prod.listing_mart.listing_attributes` b
    using (listing_id)
left join   
  etsy-data-warehouse-prod.rollups.active_listing_basics c 
    on a.listing_id=c.listing_id
left join 
  etsy-data-warehouse-prod.rollups.seller_basics d
    on c.shop_id=d.shop_id
where 
  a.date between '2023-01-01' and '2023-04-09'
group by all

------------------------------------------------------------------------
ACTIVE LISTINGS + PURCHASE METRICS OF GIFT IN TITLE LISTINGS 2023-24
------------------------------------------------------------------------
--data on views + conversion rates
with get_views as (
select
  listing_id
  , count(visit_id) as views
  , count(case when purchased_in_visit = 1 then visit_id end) as purchased_in_visit
from
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  --  (c._date between date('2024-01-01') and date('2024-04-09'))
   (_date between date('2023-01-01') and date('2023-04-09'))
group by 1 
)
select 
case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , sum(c.views) as views 
  , sum(c.purchased_in_visit) 
  , sum(c.purchased_in_visit)  / sum(c.views) as conversion_rate
from 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
left join 
  get_views c 
    on b.listing_id=c.listing_id
group by 1


--data on inventory 
select 
case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
  , avg(a.price_usd)/100 as avg_price --this table  as listing price in cents
  , count(distinct top_category) as unique_categories
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
where 
  -- (a.date between '2024-01-01' and '2024-04-09')
  (a.date between date('2023-01-01') and date('2023-04-09'))
group by 1

------------------------------------------------------------------------
YoY METRICS FOR GIFT TITLE 
--visits, acvv, conversion rate
------------------------------------------------------------------------
-- create or replace table  etsy-data-warehouse-dev.madelinecollins.gift_title_views as (
-- select 
--   extract(year from _date) as year
--   , visit_id
--   , max(case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end) as gift_title
-- from  
--   etsy-data-warehouse-prod.analytics.listing_views 
-- inner join 
--   etsy-data-warehouse-prod.listing_mart.listing_titles b
--     using (listing_id)
-- where 
--   _date >= '2020-01-01'
-- group by 1,2
-- );

--  create or replace table  etsy-data-warehouse-dev.madelinecollins.gift_title_transaction_visits as (
--  select
--   extract(year from b.date) as year
--   , b.visit_id
--   , sum(c.trans_gms_net) as trans_gms_net
-- from 
--   (select 
--    transaction_id 
--   from 
--     etsy-data-warehouse-prod.transaction_mart.all_transactions a
--    inner join 
--     etsy-data-warehouse-prod.listing_mart.listing_titles b
--        using (listing_id)
--   where 
--      date >= '2020-01-01'
--     and regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト")) a
-- inner join 
--   etsy-data-warehouse-prod.transaction_mart.transactions_visits b
--     using (transaction_id)
-- left join 
--   etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans c
--     on a.transaction_id=c.transaction_id
-- group by all
--  );
-- gift title transactions yoy

	
-- --for each year, get # of visits, acvv, conversion rate 
with yearly_metrics as (
select
  extract(year from v._date) as year
  , count(distinct gtv.visit_id) as gift_title_visits 
  , sum(case when gtv.visit_id is not null then gtv.trans_gms_net end)/count(distinct gtv.visit_id) as gift_title_acvv
  , count(distinct gtv.visit_id)/ count(distinct case when views.gift_title=1 then views.visit_id end) as gift_title_conversion_rate
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  etsy-data-warehouse-dev.madelinecollins.gift_title_transaction_visits gtv
  using (visit_id)
left join 
  etsy-data-warehouse-dev.madelinecollins.gift_title_views views
    on v.visit_id=views.visit_id
-- left join 
--   etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
--     on v.visit_id=tv.visit_id
-- left join 
--   etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans gms
--     on tv.transaction_id=gms.transaction_id
where 
  v._date >= '2020-01-01'
  -- v._date between '2023-01-01' and '2023-04-09'
  -- or v._date between '2024-01-01' and '2024-04-09'
group by 1
)
SELECT
  a.year AS current_year
  , a.gift_title_visits AS current_year_gift_visits
  , b.gift_title_visits AS previous_year_gift_visits
  , a.gift_title_acvv AS current_year_gift_acvv
  , b.gift_title_acvv AS previous_year_gift_acvv
  , a.gift_title_conversion_rate AS current_year_gift_conversion_rate
  , b.gift_title_conversion_rate AS previous_year_gift_conversion_rate
  , ((a.gift_title_visits - b.gift_title_visits) / b.gift_title_visits) * 100 AS yoy_growth_gift_visits
  , ((a.gift_title_acvv - b.gift_title_acvv) / b.gift_title_acvv) * 100 AS yoy_growth_gift_acvv
  , ((a.gift_title_conversion_rate - b.gift_title_conversion_rate) / b.gift_title_conversion_rate) * 100 AS yoy_growth_gift_conversion_rate
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
group by all
ORDER BY
  a.year;

---gift title transactions yoy
with agg as (
select
  date
  , transaction_id
  , case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_trans
from  
    etsy-data-warehouse-prod.transaction_mart.all_transactions a
   inner join 
    etsy-data-warehouse-prod.listing_mart.listing_titles b
       using (listing_id)
where date >= '2020-01-01'
)
, yearly_metrics as (
select
  extract(year from date) as year
  , count(distinct transaction_id) as transaction_id
  , count(distinct case when gift_trans =1 then transaction_id end) as gift_title_transactions
from 
  agg
where 
  date >= '2020-01-01'
  -- v._date between '2023-01-01' and '2023-04-09'
  -- or v._date between '2024-01-01' and '2024-04-09'
group by 1
)
SELECT
  a.year AS current_year
  , a.transaction_id AS current_year_transaction_id
  , b.transaction_id AS previous_year_transaction_id
  , a.gift_title_transactions AS current_year_gift_title_transactions
  , b.gift_title_transactions AS previous_year_gift_title_transactions
  , ((a.transaction_id - b.transaction_id) / b.transaction_id) * 100 AS yoy_growth_transaction_id
  , ((a.gift_title_transactions - b.gift_title_transactions) / b.gift_title_transactions) * 100 AS yoy_growth_gift_title_transactions
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
group by all
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
GIFT LISTING IMPRESSIONS
------------------------------------------------------------------------
select 
  case 
      when placement like ('%search%') then 'search'
      when placement like ('%market%') then 'market'
      when placement like ('%/c/%') or placement like ('%category%') and placement not like ('%home%') then 'category'
      when placement like ('%listing%') or placement like ('%lp%') then 'listing'
      when placement like ('%discover%') then 'discover'
      when placement like ('%home%') then 'home'
      when placement like ('%similar%') then 'similar'
      else 'other'
    end as placement
  , count(b.listing_id) as all_impressions
  , count(distinct a.listing_id) as all_listings
  , count(case when regexp_contains(a.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then b.listing_id end) as gift_listing_impressions
  , count(distinct case when regexp_contains(a.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then a.listing_id end) as gift_listings
  , count(case when not regexp_contains(a.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then b.listing_id end) as non_gift_listing_impressions
  , count(distinct case when not regexp_contains(title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then a.listing_id end) as non_gift_listings
  from 
    etsy-data-warehouse-prod.rollups.organic_impressions b 
  inner join 
     etsy-data-warehouse-prod.listing_mart.listing_titles a
      using (listing_id)
  where 
    b._date between '2024-01-01' and '2024-04-09'
    and (placement like ('%search%')
      or placement like ('%market%')
      or placement like ('%/c/%') 
      or placement like ('%category%') 
      or placement like ('%listing%') 
      or placement like ('%lp%')
      or placement like ('%discover%') 
      or placement like ('%home%')
      or placement like ('%similar%'))
group by all 
order by 2 desc
------------------------------------------------------------------------
WHERE ARE GIFT LISTING VIEWS COMING FROM
------------------------------------------------------------------------
with yearly_metrics as (
select
  extract(year from _date) as year
  , case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , ref_tag
  , count(distinct listing_id) as listings_viewed
  , count(visit_id) as views
from
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b 
    using (listing_id)
where 
  _date between '2023-01-01' and '2023-04-09' or _date between '2024-01-01' and '2024-04-09'
group by all
)
SELECT
  a.year AS current_year
  , a.gift_title
  , a.ref_tag
  , a.listings_viewed AS current_year_listings_viewed
  , b.listings_viewed AS previous_year_listings_viewed
  , a.views AS current_year_views
  , b.views AS previous_year_views
  , ((a.listings_viewed - b.listings_viewed) / b.listings_viewed) * 100 AS yoy_growth_listings_viewed
  , ((a.views - b.views) / b.views) * 100 AS yoy_growth_views
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
  and a.gift_title=b.gift_title
  and a.ref_tag=b.ref_tag
group by all
ORDER BY
  a.year;

------------------------------------------------------------------------
GIFT QUERY VISITS YOY 
------------------------------------------------------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.gift_query_visits as (
-- select
--   extract(year from _date) as year
--  , qs.visit_id
--   , max(case when qm.is_gift = 1 then 1 else 0 end) as gift_query
-- from 
--   `etsy-data-warehouse-prod.search.query_sessions_new` qs
-- join 
--   `etsy-data-warehouse-prod.rollups.query_level_metrics` qm 
--     USING (query)
-- where _date >= '2020-01-01'
-- group by 1,2
-- );

-- create or replace table etsy-data-warehouse-dev.madelinecollins.gift_query_visits_trans as (
-- select
--   a.year
--   , a.gift_query
--   , a.visit_id
--   , sum(trans_gms_net) as trans_gms_net
-- from 
--   etsy-data-warehouse-dev.madelinecollins.gift_query_visits a
-- left join 
--   etsy-data-warehouse-prod.transaction_mart.transactions_visits b 
--     using (visit_id)
-- left join 
-- 	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans c 
--     on b.transaction_id=c.transaction_id
-- group by all
-- );

with yearly_metrics as (
select
  extract(year from v._date) as year
  , count(distinct case when qv.gift_query=1 and v.converted =1 then qv.visit_id end) as total_converted_visits_gift_query
  , count(distinct qv.visit_id) as total_visits_query
  , (sum(case when qv.gift_query=1 and v.converted =1 then qv.trans_gms_net end))/(count(distinct case when qv.gift_query=1 and v.converted =1 then qv.visit_id end)) as acvv_gift_queries
  , sum(case when v.converted =1 then qv.trans_gms_net end)/ count(distinct case when v.converted =1 then qv.visit_id end) as acvv_queries
  , count(distinct case when v.converted =1 then qv.visit_id end) as total_converted_visits_query
  , count(distinct case when v.converted=1 and qv.gift_query =1 then qv.visit_id end)/ nullif(count(distinct case when qv.gift_query=1 then qv.visit_id end),0) as total_conversion_rate_gift_query
  , count(distinct case when v.converted=1 then qv.visit_id end)/ nullif(count(distinct qv.visit_id),0) as total_conversion_rate_query
from 
  etsy-data-warehouse-prod.weblog.visits v
-- left join 
--   etsy-data-warehouse-dev.madelinecollins.gift_query_visits q
--     using (visit_id)
left join 
  etsy-data-warehouse-dev.madelinecollins.gift_query_visits_trans qv
    on v.visit_id=qv.visit_id
where 
  v._date >= '2020-01-01'
  -- v._date between '2023-01-01' and '2023-04-09'
  -- or v._date between '2024-01-01' and '2024-04-09'
group by 1
)
SELECT
  a.year AS current_year
  , a.total_converted_visits_gift_query AS current_year_gift_query_visits
  , b.total_converted_visits_gift_query AS previous_year_gift_query_visits
  , a.total_converted_visits_query AS current_year_query_visits
  , b.total_converted_visits_query AS previous_year_query_visits
  
  , a.total_conversion_rate_gift_query AS current_year_conversion_rate_gift_query
  , b.total_conversion_rate_gift_query AS previous_year_conversion_rate_gift_query
  , a.total_conversion_rate_query AS current_year_conversion_rate_query
  , b.total_conversion_rate_query AS previous_year_conversion_rate_query

  , a.acvv_gift_queries AS current_year_acvv_gift_queries
  , b.acvv_gift_queries AS previous_year_acvv_gift_queries
  , a.acvv_queries AS current_year_acvv_queries
  , b.acvv_queries AS previous_year_acvv_queries

  , ((a.total_converted_visits_gift_query - b.total_converted_visits_gift_query) / nullif(b.total_converted_visits_gift_query,0)) * 100 AS yoy_growth_visits_gift_query 
  , ((a.total_converted_visits_query - b.total_converted_visits_query) / nullif(b.total_converted_visits_query,0)) * 100 AS yoy_growth_visits_query 
  , (a.acvv_gift_queries - b.acvv_gift_queries) / (nullif(b.acvv_gift_queries,0)) * 100 AS yoy_growth_acvv_gift_query
  , (a.acvv_queries - b.acvv_queries) / (nullif(b.acvv_queries,0)) * 100 AS yoy_growth_acvv_gift_query
  , (a.total_conversion_rate_gift_query - b.total_conversion_rate_gift_query) / (nullif(b.total_conversion_rate_gift_query,0)) * 100 AS yoy_growth_conversion_rate_gift_query
  , (a.total_conversion_rate_query - b.total_conversion_rate_query) / (nullif(b.total_conversion_rate_query,0)) * 100 AS yoy_growth_conversion_rate_query
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY
  a.year;


------------------------------------------------------------------------
VOLUME OF TOP 50 GIFT QUERIES YOY
------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.gift_queries as (
SELECT
  extract(year from _date) as year
  , _date
  , query
  , count(distinct visit_id) as visits
FROM 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm 
    USING (query)
WHERE 
  _date >= '2020-01-01'
  --   (_date between '2023-01-01' and '2023-04-09'
  -- or _date between '2024-01-01' and '2024-04-09')
  and is_gift > 0
group by 1,2,3);


--yoy metrics
with agg_queries as (
select 
  year
  , query
  , sum(visits) as total_visits
  , row_number () over (partition by year order by sum(visits) desc) as rank
  from etsy-data-warehouse-dev.madelinecollins.gift_queries
where
  (_date between '2023-01-01' and '2023-04-09' or _date between '2024-01-01' and '2024-04-09')
  -- _date >= '2020-01-01'
group by 1,2
)
, yearly_metrics as ( -- total visits of the top 50 queries for each year 
select 
  year
  , query
  , total_visits
from 
  agg_queries
where rank <= 50
)
SELECT
  a.year AS current_year
  , a.query
  , a.total_visits AS current_year_visits
  , b.total_visits AS previous_year_visits
  , ((a.total_visits - b.total_visits) / b.total_visits) * 100 AS yoy_growth_visits  

FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
  and a.query=b.query
group by 1,2,3,4
ORDER BY 5 desc;

-- create or replace table etsy-data-warehouse-dev.madelinecollins.gift_queries_segment as (
-- SELECT
--   extract(year from _date) as year
--   , _date
--   , query
--   , is_gift
--   , bin
--   , count(distinct visit_id) as visits
-- FROM 
--   `etsy-data-warehouse-prod.search.query_sessions_new` qs
-- JOIN 
--   `etsy-data-warehouse-prod.rollups.query_level_metrics` qm 
--     USING (query)
-- WHERE 
--   _date >= '2020-01-01'
--   --   (_date between '2023-01-01' and '2023-04-09'
--   -- or _date between '2024-01-01' and '2024-04-09')
-- group by all);

------------------------
VOLUME OF QUERIES YOY
------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.gift_queries_segment as (
-- SELECT
--   extract(year from _date) as year
--   , _date
--   , query
--   , is_gift
--   , bin
--   , count(distinct visit_id) as visits
-- FROM 
--   `etsy-data-warehouse-prod.search.query_sessions_new` qs
-- JOIN 
--   `etsy-data-warehouse-prod.rollups.query_level_metrics` qm 
--     USING (query)
-- WHERE 
--   _date >= '2020-01-01'
--   --   (_date between '2023-01-01' and '2023-04-09'
--   -- or _date between '2024-01-01' and '2024-04-09')
-- group by all);

	
--bin of queries
with yearly_metrics as 
(SELECT
  extract(year from _date) as year
  , bin
  , count(distinct visit_id) as visits
FROM 
  `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN 
  `etsy-data-warehouse-prod.rollups.query_level_metrics` qm 
    USING (query)
WHERE (_date between '2023-01-01' and '2023-04-09' or _date between '2024-01-01' and '2024-04-09')
group by all)
SELECT
  a.year AS current_year
  , a.bin
  , a.visits AS current_year_visits
  , b.visits AS previous_year_visits
  , ((a.visits - b.visits) / b.visits) AS yoy_growth_visits  
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
  and a.bin=b.bin
group by all
ORDER BY 1 desc;

------------------------
VISITS W QUERIES YOY
------------------------
with yearly_metrics as (
select
  extract(year from a._date) as year
  , count(distinct a.visit_id) as total_visits
  , count(distinct b.visit_id) as visits_with_queries
  , count(distinct case when b.gift_query=1 then b.visit_id end) as gift_query_visits
from  
  etsy-data-warehouse-prod.weblog.visits a
left join 
  etsy-data-warehouse-dev.madelinecollins.gift_query_visits b using (visit_id)
where 
  -- a._date >= '2020-01-01'
  a._date between '2023-01-01' and '2023-04-09'
  or a._date between '2024-01-01' and '2024-04-09'
group by all
)
SELECT
  a.year AS current_year
  , a.total_visits AS current_year_total_visits
  , b.total_visits AS previous_year_total_visits
  , a.visits_with_queries AS current_year_visits_with_queries
  , b.visits_with_queries AS previous_year_visits_with_queries  
  , a.gift_query_visits AS current_year_visits_gift_query_visits
  , b.gift_query_visits AS previous_year_visits_gift_query_visits
  , ((a.total_visits - b.total_visits) / nullif(b.total_visits,0)) * 100 AS yoy_growth_total_visits
  , (a.visits_with_queries - b.visits_with_queries) / (nullif(b.visits_with_queries,0)) * 100 AS yoy_growth_visits_with_queries
  , (a.gift_query_visits - b.gift_query_visits) / (nullif(b.gift_query_visits,0)) * 100 AS yoy_growth_gift_query_visits
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
group by all
ORDER BY
  a.year;


-------------------------------------------------------------------------------
BUYER SEGMENT CONVERSION AMONG GIFT QUERIES
-------------------------------------------------------------------------------
with yearly_metrics as 
(select 
  extract(year from a._date) as year
  , c.buyer_segment
-- count(distinct case when a.converted=1 then a.visit_id end) as converted_visits
-- , count(distinct case when a.converted=1 then b.visit_id end) as converted_query_visits
-- , count(distinct case when a.converted=1 and b.gift_query =1 then b.visit_id end) as converted_gift_query_visits
 , count(distinct case when a.converted=1 then a.visit_id end) as converted_visits
, count(distinct case when a.converted=1 then b.visit_id end) as converted_query_visits_trans
, count(distinct case when b.gift_query =1 and a.converted=1 then b.visit_id end) as converted_gift_query_visits_trans
from  
  etsy-data-warehouse-prod.weblog.visits a
left join 
  etsy-data-warehouse-dev.madelinecollins.gift_query_visits b 
    using (visit_id)
inner join  --- only want transactions
  etsy-data-warehouse-prod.rollups.visits_w_segments c -- use this table bc only goes back to 2023 and thats all i need, gives segment from day of purchase
    on a.visit_id=c.visit_id
where 
  a._date between '2023-01-01' and '2023-04-09'
  or a._date between '2024-01-01' and '2024-04-09'
group by all
)
select
 a.year AS current_year
 , a.buyer_segment
 , a.converted_visits as current_year_converted_visits
 , b.converted_visits as previous_year_converted_visits
 , a.converted_query_visits_trans as current_year_converted_query_visits_trans
 , b.converted_query_visits_trans as previous_year_converted_query_visits_trans
 , a.converted_gift_query_visits_trans as current_year_converted_gift_query_visits_trans
 , b.converted_gift_query_visits_trans as previous_year_converted_gift_query_visits_trans
 , ((a.converted_visits - b.converted_visits) / nullif((b.converted_visits),0)) * 100 AS yoy_growth_converted_visits
 , ((a.converted_query_visits_trans - b.converted_query_visits_trans) / nullif((b.converted_query_visits_trans),0)) * 100 AS yoy_growth_converted_query_visits_trans
 , ((a.converted_gift_query_visits_trans - b.converted_gift_query_visits_trans) / nullif((b.converted_gift_query_visits_trans),0)) * 100 AS yoy_growth_converted_gift_query_visits_trans

FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
  and a.buyer_segment=b.buyer_segment
group by all
ORDER BY
  a.year;
-------------------------------------------------------------------------------
VOLUME OF SEARCH SOURCE -- 2024 ONLY BC DONT HAVE DATA TILL SECOND HALF OF 2023
-------------------------------------------------------------------------------
select
  extract(year from a._date) as year
 , qm.is_gift -- regexp logic flag.
 , case 
    when search_source like ('hp_%') OR search_source like ('homepage%')  then 'homepage'
    when search_source in ('search_bar','auto') then 'search_bar, auto -- buyer typed'
    when search_source like ('catnav%') then 'catnav'
    when search_source like ('cat_hobby%') then 'cat_hobby'
    when search_source like ('hub_stashgrid_module%') then 'hub_stashgrid_module'
    when search_source like ('s2_qi%') then 'query_ingresses'
    when search_source like ('s2_rbl%') then 'morelikethis_button'
    when search_source IS NULL THEN NULL
    else split(search_source, '-')[safe_offset(0)]
  end as search_source
, count(distinct a.visit_id) as visits
, count(distinct a.visit_id) / SUM(COUNT(DISTINCT a.visit_id)) OVER (PARTITION BY qm.is_gift) AS perc_visits
from 
  etsy-data-warehouse-prod.search.query_sessions_new a
join `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
inner join
  etsy-data-warehouse-prod.weblog.visits b
    using (visit_id, _date)
where 
  ((b._date between '2023-01-01' and '2023-04-09') or (b._date between '2024-01-01' and '2024-04-09'))
  and b.platform in ('mobile_web', 'desktop')
group by 1,2,3
QUALIFY count(distinct a.visit_id) / SUM(COUNT(DISTINCT a.visit_id)) OVER (PARTITION BY qm.is_gift) > 0.01 -- exclude weird edge cases that make up <1% of traffic
order by 4 desc
;
------------------------------------------------------------------------
CREATE GIFT INTENT VISITS TABLE FOR DENOMINATOR OF TIAG CONVERSION RATE
------------------------------------------------------------------------
--create tables for each year to speed up 
create or replace table etsy-data-warehouse-dev.madelinecollins.visits_since_jan1_2022 as (
select
	b.visit_id
  , b._date
  , b.user_id
  , b.landing_event
  , b.landing_event_url
  ,	case when b.landing_event = "market" then lower(regexp_replace(regexp_replace(regexp_substr(b.landing_event_url, "market/([^?]*)"), "_", " "), "\\%27", "")) else null end as landing_market_query,
	case when b.landing_event like "view%listing%" then safe_cast(regexp_substr(b.landing_event_url, "listing\\/(\\d*)") as int64) else null end as landing_listing_id,
	case when b.landing_event = "search" then regexp_replace(regexp_replace(regexp_substr(lower(b.landing_event_url), "q=([a-z0-9%+]+)"),"\\\\+"," "),"%20"," ") else null end as landing_search_query,
	case when b.landing_event = "finds_page" then lower(regexp_substr(b.landing_event_url, "featured\\/([^\\/\\?]+)")) else null end as landing_gg_slug
from `etsy-data-warehouse-prod.weblog.visits` b
where 
  extract(year from b._date) = 2022
	and b.platform in ("boe","desktop","mobile_web") --remove soe
	and b.is_admin_visit != 1 --remove admin
); 
--------------------------------
begin 
create or replace temporary table visits as (
with visits as (
select
	b._date,
	b.visit_id,
	b.user_id,
  b.landing_event,
	b.landing_event_url,
	v.utm_campaign,
	case when b.landing_event = "market" then lower(regexp_replace(regexp_replace(regexp_substr(b.landing_event_url, "market/([^?]*)"), "_", " "), "\\%27", "")) else null end as landing_market_query,
	case when b.landing_event like "view%listing%" then safe_cast(regexp_substr(b.landing_event_url, "listing\\/(\\d*)") as int64) else null end as landing_listing_id,
	case when b.landing_event = "search" then regexp_replace(regexp_replace(regexp_substr(lower(b.landing_event_url), "q=([a-z0-9%+]+)"),"\\\\+"," "),"%20"," ") else null end as landing_search_query,
	case when b.landing_event = "finds_page" then lower(regexp_substr(b.landing_event_url, "featured\\/([^\\/\\?]+)")) else null end as landing_gg_slug
from
	etsy-data-warehouse-dev.madelinecollins.visits_since_jan1_2022 b
left join 
	-- data around channels (top_channel, utm_campaign, etc.) should be taken from buyatt_mart which is canonical marketing source
	`etsy-data-warehouse-prod.buyatt_mart.visits` v
using(visit_id)
where 
	(b.user_id is null or b.user_id not in (
		select user_id from `etsy-data-warehouse-prod.rollups.seller_basics` where active_seller_status = 1)
		) --remove sellers
), first_action as (
select
	v.visit_id,
	e.sequence_number,
	e.event_type,
	e.listing_id,
	lower(regexp_substr(e.url,"\\/c\\/+([^?]*)")) as category_page,
	lower(regexp_replace(regexp_replace(regexp_substr(lower(url), "q=([a-z0-9%+]+)"),"\\+"," "),"%20"," ")) as search_query,
	lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "market/([^?]*)"), "_", " "), "\\%27", "")) as market_query,
	lower(regexp_substr(url, "featured\\/([^\\/\\?]+)")) as gift_guide_slug,
	case when event_type in ("category_page", "category_page_hub") then regexp_replace(split(regexp_substr(url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(0)],"-","_") end as cat_page_top_cat,
	case when event_type in ("category_page", "category_page_hub") then regexp_replace(split(regexp_substr(url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(1)],"-","_") end as cat_page_second_cat
from 
	visits v
join 
	`etsy-data-warehouse-prod.weblog.events` e 
using (visit_id)
where (
	e.event_type like "view%listing%" --includes unavailable or sold listing view
	or e.event_type like "gift_mode%"
	or e.event_type in ("search","category_page","category_page_hub","market", "shop_home", "finds_page", 
		"giftreceipt_view","gift_recipientview_boe_view","gift_receipt")
	)
and e.page_view = 1
and e._date > '2022-01-01'
qualify row_number() over(partition by v.visit_id order by e.sequence_number) = 1  --first action event only
)
select 
	v.*,
	fa.event_type as first_action_type,
	fa.listing_id as first_action_listing_id,
	fa.category_page as first_action_category_page,
	fa.search_query as first_action_search_query,
	fa.market_query as first_action_market_query,
	fa.gift_guide_slug as first_action_gg_slug,
	fa.cat_page_top_cat,
	fa.cat_page_second_cat,
	coalesce(safe_cast(v.landing_listing_id as int64),safe_cast(fa.listing_id as int64)) as listing_id_clean,
	lower(coalesce(v.landing_market_query,v.landing_search_query, fa.market_query, fa.search_query)) as query_clean,
	lower(coalesce(v.landing_gg_slug, fa.gift_guide_slug)) as gg_slug_clean
from 
	visits v
left join 
	first_action fa 
on 
	v.visit_id = fa.visit_id 
);
create or replace temporary table query_sessions_temp as (
with queries as (
select
	q.query_raw,
	c.taxonomy_id,
	c.full_path as query_full_path,
	split(c.full_path, ".")[safe_offset(0)] as query_top_cat,
	split(c.full_path, ".")[safe_offset(1)] as query_second_cat,  
	case when (regexp_contains(q.query_raw, "(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)")
		or regexp_contains(q.query_raw, "(\?i)\\boccasion|\\banniversary|\\bbirthday|\\bmothers day|\\bfathers day|\\bchristmas present")) then 1
	else 0 end as gift_query,
	row_number() over (partition by query_raw order by count(*) desc) as rn,
	count(*) as session_count,
from
	`etsy-data-warehouse-prod.search.query_sessions_new` q
join 
	`etsy-data-warehouse-prod.structured_data.taxonomy_latest` c on q.classified_taxonomy_id = c.taxonomy_id
where 
	q._date >= '2022-01-01' -- get a full year of classifications to get maximal coverage
and q.query_raw in (
	select distinct query_clean from visits
	)
group by 
	1,2,3,4,5
)
select 
	* 
from 
	queries 
where rn = 1	-- most common categorization for a given query
);
create or replace temporary table listing_attributes_temp as (
select 
	distinct l.listing_id,
	l.taxonomy_id,
	c.full_path as listing_full_path,
	split(c.full_path, ".")[safe_offset(0)] as listing_top_cat,
	split(c.full_path, ".")[safe_offset(1)] as listing_second_cat,
	ls.title,
	case when regexp_contains(ls.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)") then 1 else 0 end as gift_title
from `etsy-data-warehouse-prod.listing_mart.listing_attributes` l
join `etsy-data-warehouse-prod.structured_data.taxonomy_latest` c using (taxonomy_id)
left join `etsy-data-warehouse-prod`.listing_mart.listing_titles ls on l.listing_id = ls.listing_id
where l.listing_id in (
	select listing_id_clean from visits
	) 
);
create or replace temporary table purchases as (
with all_purch as (
select
	a.date 
	, tv.visit_id
	, r.receipt_id
	, a.transaction_id 
	, a.listing_id
	, t.trans_gms_net 
	, a.is_gift 
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
where 
	a.date > '2022-01-01'
), gtt as (
select 
	a.* 
	, max(case when regexp_contains(l.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end) as gift_title
from 
	all_purch a 
left join 
	`etsy-data-warehouse-prod`.listing_mart.listing_titles l 
using(listing_id) 
group by 1,2,3,4,5,6,7
), searches as (
SELECT
	distinct _date, visit_id
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE _date > '2022-01-01'
and is_gift > 0
)
select 
	a.date 
	, a.visit_id 
	, sum(trans_gms_net) as total_gms 
	, sum(case when is_gift > 0 or gift_title > 0 or b.visit_id is not null then trans_gms_net end) as gift_gms
	, count(distinct receipt_id) as total_orders
	, count(distinct case when is_gift > 0 or gift_title > 0 or b.visit_id is not null then receipt_id end) as gift_orders
from 
	gtt a 
left join 
	searches b 
on 
	a.visit_id = b.visit_id
group by 1,2
)
;
create or replace temporary table category_visits_full as (
select
	b._date,
	b.visit_id,
	b.user_id,
	b.landing_event,
	b.landing_event_url,
	b.utm_campaign,
	b.first_action_type,
	b.first_action_category_page,
	b.listing_id_clean,
	b.query_clean,
	b.gg_slug_clean,
	l.listing_top_cat,
	l.listing_second_cat,
	l.gift_title,
	q.query_full_path,
	q.query_top_cat,
	q.query_second_cat,
	q.gift_query,
from 
	visits b
left join  
	listing_attributes_temp l on listing_id_clean = l.listing_id 
left join 
	query_sessions_temp q on query_clean = lower(q.query_raw)
left join 
	purchases p 
on 
	b.visit_id = p.visit_id
);
create or replace temporary table classified_visits as (
select
	v.*
	,case when (landing_event like "gift_mode%" or landing_event in ("gift_recipientview_boe_view","giftreceipt_view","gift_receipt")) then 1
	when (first_action_type like "gift_mode%" or landing_event in ("gift_recipientview_boe_view","giftreceipt_view","gift_receipt")) then 1
	when utm_campaign like "%gift%" then 1
	when first_action_category_page like "%gift%" then 1
	when gift_query > 0 then 1
	when gg_slug_clean like "%gift%" then 1
	when gift_title > 0 then 1
	else 0 end as gift_visit
from 
	category_visits_full v
)
;

create or replace table `etsy-data-warehouse-dev.madelinecollins.gift_intent_visits_all_2022` as (
select 
	_date
	, visit_id 
from 
	classified_visits 
where 
	gift_visit > 0
)
;
end
------------------------------------------------------------------------
YOY FOR TIAG ORDERS
------------------------------------------------------------------------
-- create or replace table  etsy-data-warehouse-dev.madelinecollins.tiag_order_visits as (
-- select 
--   a.is_gift
--   , a.date
--   , b.visit_id
--   , sum(c.trans_gms_net) as trans_gms_net
-- from 
--   etsy-data-warehouse-prod.transaction_mart.all_transactions a
-- inner join 
--   etsy-data-warehouse-prod.transaction_mart.transactions_visits b
--   using (transaction_id)
-- left join 
--   etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans c
--     on a.transaction_id=c.transaction_id
-- where 
--   a.date >= '2020-01-01' 
-- group by 1,2,3
-- );
	
-- create or replace table `etsy-data-warehouse-dev.madelinecollins.gift_intent_visits_agg` as (
-- select * from `etsy-data-warehouse-dev.madelinecollins.gift_intent_visits_all_2024`
-- union all
-- select * from `etsy-data-warehouse-dev.madelinecollins.gift_intent_visits_all_2023`
-- union all
-- select * from `etsy-data-warehouse-dev.madelinecollins.gift_intent_visits_all_2022`
-- );

with yearly_metrics as (
select
  extract(year from v._date) as year
  , count(distinct v.visit_id) as total_visits
  -- , count(distinct intent.visit_id) as gift_intent_visits 
  , count(distinct case when is_gift=1 then tiag.visit_id end) as tiag_visits 
  , sum(case when tiag.visit_id is not null and tiag.is_gift=1 then v.total_gms end)/nullif(count(distinct case when tiag.is_gift=1 then tiag.visit_id end),0) as tiag_acvv
  , sum(case when tiag.visit_id is not null and tiag.is_gift=1 then tiag.trans_gms_net end)/nullif(count(distinct case when tiag.is_gift=1 then tiag.visit_id end),0) as tiag_acvv_trans
 , count(distinct case when tiag.is_gift=1 then tiag.visit_id end)/ nullif(count(distinct intent.visit_id),0) as tiag_conversion_rate
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  `etsy-data-warehouse-dev.madelinecollins.gift_intent_visits_agg` intent
    using (visit_id)
left join 
  etsy-data-warehouse-dev.madelinecollins.tiag_order_visits tiag
    on v.visit_id=tiag.visit_id
where 
  v._date >= '2020-01-01'
  -- v._date between '2023-01-01' and '2023-04-09'
  -- or v._date between '2024-01-01' and '2024-04-09'
group by 1
)
SELECT
  a.year AS current_year
  , a.tiag_visits AS current_year_tiag_visits
  , b.tiag_visits AS previous_year_tiag_visits
  , a.tiag_acvv AS current_year_tiag_acvv
  , b.tiag_acvv AS previous_year_tiag_acvv
  , a.tiag_acvv_trans AS current_year_tiag_acvv_trans
  , b.tiag_acvv_trans AS previous_year_tiag_acvv_trans
  , a.tiag_conversion_rate AS current_year_tiag_conversion_rate
  , b.tiag_conversion_rate AS previous_year_tiag_conversion_rate
  , ((a.tiag_visits - b.tiag_visits) / nullif(b.tiag_visits,0)) * 100 AS yoy_growth_tiag_visits  
  , ((a.tiag_acvv - b.tiag_acvv) / nullif(b.tiag_acvv,0)) * 100 AS yoy_growth_tiag_acvv
  , ((a.tiag_acvv_trans - b.tiag_acvv_trans) / nullif(b.tiag_acvv_trans,0)) * 100 AS yoy_growth_tiag_acvv_trans
  , ((a.tiag_conversion_rate - b.tiag_conversion_rate) / nullif(b.tiag_conversion_rate,0)) * 100 AS yoy_growth_tiag_conversion_rate
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
group by 1,2,3,4,5,6,7,8,9
ORDER BY
  a.year;

---query to check 
select
  extract(year from date) as year
, is_gift
, count(distinct transaction_id) as transactions
, count(distinct receipt_id) as receipts
from 
etsy-data-warehouse-prod.transaction_mart.all_transactions 
where date between '2023-01-01' and '2023-04-09' or date between '2024-01-01' and '2024-04-09'
group by 1,2
------------------------------------------------------------------------
YOY TRANSACTIONS
------------------------------------------------------------------------
with yearly_metrics as (
select
  extract(year from a.date) as year
  , count(distinct a.transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions a
where 
  (a.date between '2023-01-01' and '2023-04-09' or a.date between '2024-01-01' and '2024-04-09')
  -- A.date >= '2020-01-01'
group by all
)
SELECT
 a.year AS current_year
  , sum(a.transactions) as current_year_trans
  , sum(b.transactions) as previous_year_trans
  , ((a.transactions - b.transactions) / nullif(b.transactions,0)) * 100 AS yoy_growth_transactions 
FROM
  yearly_metrics a
JOIN
  yearly_metrics b
ON
  a.year = b.year + 1
group by all
ORDER BY
  a.year;
