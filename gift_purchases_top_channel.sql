-------------------------
purchases 
-------------------------
  with purchases as (
select
	date(r.creation_tsz) as _date 
	, tv.visit_id
	, tv.top_channel
	, r.receipt_id
	, a.transaction_id 
	, v.title
	, a.listing_id
	, a.is_gift 
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
	`etsy-data-warehouse-prod.schlep_views.transactions_vw` v 
on 
	v.transaction_id = a.transaction_id
where 
	a.date >= current_date-30
)
,  gift_searches as (
SELECT
	distinct _date, visit_id
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE 
	_date >= current_date-30
and is_gift > 0
)
select
	 top_channel
	, count(distinct case when gift_title >0 then receipt_id end) gift_title_purchases --counting receipts bc on listing level 
	, count(distinct case when is_gift >0 then receipt_id end) is_gift_purchases 
	, count(distinct case when b.visit_id is not null then receipt_id end) gift_query_purchases 
	, count(distinct case when gift_title >0 or is_gift >0 or b.visit_id is not null then receipt_id end) as broad_gifting_purchases
from 
	purchases a
left join 
	gift_searches b
		using (_date, visit_id)
group by all

------------------------------------
--listing views by channel
------------------------------------
with get_views as (
select
  top_channel
  , listing_id
  , count(a.visit_id) as views
  , count(case when a.purchased_after_view = 1 then a.visit_id end) as purchased_in_visit
from
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using(visit_id, _date)
where 
  --  (_date between date('2024-01-01') and _date('2024-06-06'))
  a._date >= current_date-1
  and b._date >= current_date-1
group by all 
)
select
  top_channel
  , case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as unique_listings
  , sum(a.views) as total_listing_views
  , sum(a.purchased_in_visit) as total_purchases
from 
  get_views a 
left join etsy-data-warehouse-prod.listing_mart.listing_titles b using (listing_id)
group by all
