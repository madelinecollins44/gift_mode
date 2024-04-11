------------------------------------------------------------------------
GIFT TITLE 
------------------------------------------------------------------------
select 
	case when regexp_contains(b.title, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end as gift_title
  , count(distinct a.listing_id) as active_listings
  , avg(a.price_usd)/100 as avg_price --this table  as listing price in cents
  , count(distinct top_category) as unique_categories
  , count(c.visit_id) as views 
  , count(case when c.purchased_in_visit = 1 then c.visit_id end) as purchased_in_visit 
  , count(case when c.purchased_in_visit = 1 then c.visit_id end) / count(c.visit_id) as conversion_rate
from 
  etsy-data-warehouse-prod.incrementals.listing_daily a
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_titles b
    using(listing_id)
left join 
  etsy-data-warehouse-prod.analytics.listing_views c 
    on a.listing_id=c.listing_id
where 
  (a.date between '2024-01-01' and '2024-04-09')
  -- (a.date between date('2023-01-01') and date('2023-04-09'))
  and (c._date between date('2024-01-01') and date('2024-04-09'))
  -- and (c._date between date('2023-01-01') and date('2023-04-09'))
group by 1
----------------------------------------------------
GIFT SEARCHING BEHAVIOR 
----------------------------------------------------

----------------------------------------------------
WHAT IS THE IMPACT OF LESS TIAG GMS?
----------------------------------------------------
