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
