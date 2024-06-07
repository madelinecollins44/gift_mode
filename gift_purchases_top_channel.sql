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
	 , count(distinct receipt_id) as total_receipts
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
  a._date
  , f.reporting_channel_group
  , a.listing_id
  , case when regexp_contains(c.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , avg(overall_giftiness) as giftiness_score
  , count(a.visit_id) as views
from
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  etsy-data-warehouse-prod.weblog.visits b 
    using(visit_id, _date)
inner join 
  etsy-data-warehouse-prod.buyatt_mart.channel_dimensions f
    on b.utm_medium=f.utm_medium
    and b.utm_campaign=f.utm_campaign
    and b.top_channel=f.top_channel
    and b.second_channel=f.second_channel
    and b.third_channel=f.third_channel
left join
  etsy-data-warehouse-prod.listing_mart.listing_titles c 
    on a.listing_id=c.listing_id
left join 
  etsy-data-warehouse-prod.knowledge_base.listing_giftiness d
    on a.listing_id=d.listing_id
    and a._date=d._date
where 
  a._date >= current_date-30
  and b._date >= current_date-30
group by all 
)
select
  reporting_channel_group
  , count(distinct listing_id) as unique_listings_viewed
  , sum(views) as total_views
  , count(distinct case when gift_title > 0 then listing_id end ) as unique_gift_title_listings_viewed
  , sum(case when gift_title > 0 then views end) as total_gift_title_views
  , count(distinct case when giftiness_score >= 0.61 then listing_id end) as unique_gifty_listings_viewed
  , sum(case when giftiness_score >= 0.61 then views end) as total_gifty_views
from get_views
group by all
