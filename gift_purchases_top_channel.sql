-------------------------
purchases 
-------------------------
with purchases as (
select
	date(r.creation_tsz) as _date 
	, tv.visit_id
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
    on a.transaction_id = tv.transaction_id
left join 
	`etsy-data-warehouse-prod.schlep_views.transactions_vw` v 
  on v.transaction_id = a.transaction_id
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
case 
                when top_channel in ('direct', 'dark', 'internal', 'seo') then initcap(top_channel)
                when top_channel like 'social_%' then 'Non-Paid Social'
                when top_channel like 'email%' then 'Email'
                when top_channel like 'push_%' then 'Push'
                when top_channel in ('us_paid','intl_paid') then 
                        case when (second_channel like '%gpla' or second_channel like '%bing_plas' or second_channel like '%css_plas') then 'PLA'
                                when (second_channel like '%_ppc' or second_channel like 'admarketplace') then 
                                        case when third_channel like '%_brand' then 'SEM - Brand'
                                                else 'SEM - Non-Brand' 
                                        end
                                when second_channel='affiliates'  then 'Affiliates'
                                when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
                                when second_channel like '%native_display' then 'Display'
                                when second_channel in ('us_video','intl_video') then 'Video'
                                else 'Other Paid'
                        end
                else 'Other Non-Paid'
        end as reporting_channel_group
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
inner join 
  etsy-data-warehouse-prod.weblog.visits c
    on a.visit_id=c.visit_id
where c._date >= current_date-30
group by all

------------------------------------
--listing views by channel
------------------------------------
with get_views as (
select
  a._date
, case 
                when top_channel in ('direct', 'dark', 'internal', 'seo') then initcap(top_channel)
                when top_channel like 'social_%' then 'Non-Paid Social'
                when top_channel like 'email%' then 'Email'
                when top_channel like 'push_%' then 'Push'
                when top_channel in ('us_paid','intl_paid') then 
                        case when (second_channel like '%gpla' or second_channel like '%bing_plas' or second_channel like '%css_plas') then 'PLA'
                                when (second_channel like '%_ppc' or second_channel like 'admarketplace') then 
                                        case when third_channel like '%_brand' then 'SEM - Brand'
                                                else 'SEM - Non-Brand' 
                                        end
                                when second_channel='affiliates'  then 'Affiliates'
                                when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
                                when second_channel like '%native_display' then 'Display'
                                when second_channel in ('us_video','intl_video') then 'Video'
                                else 'Other Paid'
                        end
                else 'Other Non-Paid'
        end as reporting_channel_group
  , a.listing_id
  , case when regexp_contains(c.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , avg(overall_giftiness) as giftiness_score
  , count(a.visit_id) as views
from
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  etsy-data-warehouse-prod.weblog.visits b 
    using(visit_id, _date)
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
