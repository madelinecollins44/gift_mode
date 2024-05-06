---------------------------------------------
VISITS + GMS IN LAST 30 DAYS
--------------------------------------------
--overall visits 
select count(distinct visit_id), sum(total_gms) from etsy-data-warehouse-prod.weblog.visits where _date>= current_date-30

--visits with query 
with raw as 
  (select distinct visit_id from etsy-data-warehouse-prod.search.query_sessions_new where _date >= current_date-30)
select 
count(distinct visit_id) as unique_visits_with_query
, sum(b.total_gms) as search_gms
from raw a
inner join etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where b._date>= current_date-30  

---------------------------------------------
TOP QUERIES IN TIAG ORDERS
--------------------------------------------
with tiag_orders as (
select 
  a.is_gift
  , b.visit_id
  , sum(c.trans_gms_net) as trans_gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions a
inner join 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits b
  using (transaction_id)
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans c
    on a.transaction_id=c.transaction_id
where 
  a.date >= current_date-30
  and a.is_gift=1
group by all
)
select 
query 
, count(distinct a.visit_id) as unique_visits
, count(a.visit_id) as total_searches
from 
  tiag_orders a
inner join 
  etsy-data-warehouse-prod.search.query_sessions_new b
    using (visit_id)
where 
b._date>= current_date-30 
--and query not like ('%gift%')
group by all
order by 3 desc

	
------------------------------------
QUERIES BY GIFTINESS SCORE
------------------------------------
with raw as (
select
  visit_id
  , avg(overall_giftiness)
from 
  etsy-data-warehouse-prod.knowledge_base.query_giftiness a
inner join 
  etsy-data-warehouse-prod.search.query_sessions_new b 
    on a.query=b.query
    and a._date=b._date -- gets avg giftiness score for queries from visit date
where a._date >= current_date-30 and b._date >= current_date-30
group by all
having avg(overall_giftiness) >= 0.51
)
select 
count(distinct visit_id) as unique_visits
, sum(total_gms) as total_gms
from 
  raw a
inner join 
  etsy-data-warehouse-prod.weblog.visits b
    using (visit_id)
where b._date >= current_date-30

--examples of queries by giftiness score
select
  a.query
  , count(visit_id) as sessions
  , avg(overall_giftiness) as avg_score
from 
  etsy-data-warehouse-prod.knowledge_base.query_giftiness a
inner join 
  etsy-data-warehouse-prod.search.query_sessions_new b 
    on a.query=b.query
    and a._date=b._date -- gets avg giftiness score for queries from visit date
where a._date >= current_date-5 and b._date >= current_date-5
group by all
having 
  avg(overall_giftiness) >= 0.41 
  and avg(overall_giftiness) <= 0.51 
  and count(visit_id) >= 10000
order by 2 desc

------------------------------------
GIFT QUERY
------------------------------------
with get_visits as (
SELECT
	visit_id
  , max(case when is_gift > 0 then 1 else 0 end) as is_gift
  , max(case when is_holiday > 0 then 1 else 0 end) as is_holiday
  , max(case when is_occasion > 0 then 1 else 0 end) as is_occasion
  , max(case when is_gift > 0 or is_holiday >0 or (is_occasion > 0 and is_gift > 0) then 1 else 0 end) as is_gift_holiday_giftoccasion
  , max(case when is_gift > 0 or is_holiday >0 or is_occasion > 0 or regexp_contains(qm.query, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end) as is_gift_holiday_occasion_regex
  , max(case when regexp_contains(qm.query, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト") then 1 else 0 end) as regex_gift   
  , max(case when regexp_contains(qm.query, "(\?i)\\bchristmas|\\bhanukkah|\\bvalentine|mothers day|fathers day|\\bbirthday|\\bgraduation|\\bdiwali|\\bkwanzaa|\\bchanukah|\\bwedding|\\bretirement") then 1 else 0 end) as regex_gifting_holidays_occasions
  , max(case when qm.query like ('%card%') and qm.query not like ('%business%') and qm.query not like ('%tarot%')and qm.query not like ('%playing%')and qm.query not like ('%playing%')and qm.query not like ('%deck%')  then 1 else 0 end) as greeting_card
  , max(case when regexp_contains(qm.query, "(\?i)\\bpersonalize|\\bunique|\\bhandmade|\\bcustom") then 1 else 0 end) as gift_attributes
  , max(case when regexp_contains(qm.query, "(\?i)\\bearring|\\bnecklace|\\bbracelet|\\baccessory|\\bjewelry|\\bcup|\\bmug|\\bcandle") then 1 else 0 end) as gift_items
  , max(case when regexp_contains(qm.query, "(\?i)\\bcarepackage|\\bcare package") then 1 else 0 end) as carepackage
  , max(case when regexp_contains(qm.query, "(\?i)\\bgiftbox|\\bgift box") then 1 else 0 end) as giftbox
  , max(case when regexp_contains(qm.query, "(\?i)\\bpresent") then 1 else 0 end) as present
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE 
	_date >= current_date - 30
group by all
)
select
count(distinct case when is_gift =1 then a.visit_id end) as visits_is_gift
, count(distinct case when is_holiday =1 then a.visit_id end) as visits_is_holiday
, count(distinct case when is_occasion =1 then a.visit_id end) as visits_is_occasion
, count(distinct case when is_gift_holiday_giftoccasion =1 then a.visit_id end) as visits_is_gift_holiday_giftoccasion
, count(distinct case when is_gift_holiday_occasion_regex =1 then a.visit_id end) as visits_is_gift_holiday_occasion_regex
, count(distinct case when regex_gift =1 then a.visit_id end) as visits_regex_gift
, count(distinct case when regex_gifting_holidays_occasions =1 then a.visit_id end) as visits_regex_gifting_holidays_occasions
, count(distinct case when greeting_card =1 then a.visit_id end) as visits_greeting_card
, count(distinct case when gift_attributes =1 then a.visit_id end) as visits_gift_attributes
, count(distinct case when gift_items =1 then a.visit_id end) as visits_gift_items
, count(distinct case when carepackage =1 then a.visit_id end) as visits_carepackage
, count(distinct case when giftbox =1 then a.visit_id end) as visits_giftbox
, count(distinct case when present =1 then a.visit_id end) as visits_present
, sum(case when is_gift =1 then b.total_gms end) as gms_is_gift
, sum(case when is_holiday =1 then b.total_gms end) as gms_is_holiday
, sum(case when is_occasion =1 then b.total_gms end) as gms_is_occasion
, sum(case when is_gift_holiday_giftoccasion =1 then b.total_gms end) as gms_is_gift_holiday_giftoccasion
, sum(case when is_gift_holiday_occasion_regex =1 then b.total_gms end) as gms_is_gift_holiday_occasion_regex
, sum(case when regex_gift =1 then b.total_gms end) as gms_regex_gift
, sum(case when regex_gifting_holidays_occasions =1 then b.total_gms end) as gms_regex_gifting_holidays_occasions
, sum(case when greeting_card =1 then b.total_gms end) as gms_greeting_card
, sum(case when gift_attributes =1 then b.total_gms end) as gms_gift_attributes
, sum(case when gift_items =1 then b.total_gms end) as gms_gift_items
, sum(case when carepackage =1 then b.total_gms end) as gms_carepackage
, sum(case when giftbox =1 then b.total_gms end) as gms_giftbox
, sum(case when present =1 then b.total_gms end) as gms_present
from 
  get_visits a
inner join 
  etsy-data-warehouse-prod.weblog.visits b 
    using (visit_id)
where b._date>= current_date-30  



------------------------------------------------------------------------
LISTING RESULTS ON FIRST PAGE OF SEARCH RESULTS
------------------------------------------------------------------------

begin 
--etsy-bigquery-adhoc-prod._scripte3232a8ec45edeac65a992896a79cdf0bfd04201.gifty_score
create or replace temporary table gifty_score as (
select 
  query
  , listing_id
  , score
from 
  etsy-data-warehouse-dev.madelinecollins.gifty_score_first_search_pg
group by all
);

--etsy-bigquery-adhoc-prod._scripte3232a8ec45edeac65a992896a79cdf0bfd04201.query_count
create or replace temporary table query_count as (
select distinct 
  query
  , visit_id
from 
  etsy-data-warehouse-dev.madelinecollins.gifty_score_first_search_pg 
group by all
);

--etsy-bigquery-adhoc-prod._scripte3232a8ec45edeac65a992896a79cdf0bfd04201.visit_level
create or replace temporary table visit_level as (
select 
  a.visit_id
  , avg(b.score) as score
from query_count a
inner join gifty_score b 
      using (query)
group by all
having count(a.query) >= 10000
);

--etsy-bigquery-adhoc-prod._scripte3232a8ec45edeac65a992896a79cdf0bfd04201.agg
--etsy-bigquery-adhoc-prod._script278c602e50b7a6eb202fab6fd75b306ec7f6c1d9.agg_above
create or replace temporary table agg as (
select
count(distinct case when a.score <= 0.2 then a.visit_id end) as unique_visits_lessthan2
, count(distinct case when a.score <= 0.3 then a.visit_id end) as unique_visits_lessthan3
, count(distinct case when a.score <= 0.4 then a.visit_id end) as unique_visits_lessthan4
, count(distinct case when a.score <= 0.5 then a.visit_id end) as unique_visits_lessthan5
, count(distinct case when a.score <= 0.6 then a.visit_id end) as unique_visits_lessthan6
, count(distinct case when a.score <= 0.7 then a.visit_id end) as unique_visits_lessthan7
, count(distinct case when a.score <= 0.8 then a.visit_id end) as unique_visits_lessthan8
, count(distinct case when a.score <= 0.9 then a.visit_id end) as unique_visits_lessthan9
, sum(case when a.score <= 0.2 then b.total_gms end) as total_gms_lessthan2
, sum(case when a.score <= 0.3 then b.total_gms end) as total_gms_lessthan3
, sum(case when a.score <= 0.4 then b.total_gms end) as total_gms_lessthan4
, sum(case when a.score <= 0.5 then b.total_gms end) as total_gms_lessthan5
, sum(case when a.score <= 0.6 then b.total_gms end) as total_gms_lessthan6
, sum(case when a.score <= 0.7 then b.total_gms end) as total_gms_lessthan7
, sum(case when a.score <= 0.8 then b.total_gms end) as total_gms_lessthan8
, sum(case when a.score <= 0.9 then b.total_gms end) as total_gms_lessthan9
from 
  visit_level a
inner join 
  etsy-data-warehouse-prod.weblog.visits b 
  using(visit_id)
where b._date>= current_date-5
group by all
);

end


	
------------------------------------------------
COMBO
------------------------------------------------
begin 
create or replace temporary table query_giftiness as (
select
  visit_id
  , avg(overall_giftiness) as score
from 
  etsy-data-warehouse-prod.knowledge_base.query_giftiness a
inner join 
  etsy-data-warehouse-prod.search.query_sessions_new b 
    on a.query=b.query
    and a._date=b._date -- gets avg giftiness score for queries from visit date
where a._date >= current_date-30 
and b._date >= current_date-30
group by all
having avg(overall_giftiness) >= 0.51
);

create or replace temporary table keywords as (
SELECT
	visit_id
    , max(case when regexp_contains(qm.query, "(\?i)\\bgift|\\bcadeau|\\bregalo|\\bgeschenk|\\bprezent|ギフト|") then 1 else 0 end) as regex_gift
    , max(case when regexp_contains(qm.query, "(\?i)\\bchristmas|\\bhanukkah|\\bvalentine|\\bmothers day|\\bfathers day|\\bbirthday|\\bgraduation|\\bdiwali|\\bkwanzaa|\\bchanukah|\\bwedding|\\bretirement") then 1 else 0 end) as regex_gifting_holidays_occasions
  , max(case when qm.query like ('%card%') and qm.query not like ('%business%') and qm.query not like ('%tarot%')and qm.query not like ('%playing%')and qm.query not like ('%playing%')and qm.query not like ('%deck%')  then 1 else 0 end) as greeting_card
  , max(case when regexp_contains(qm.query, "(\?i)\\bcarepackage|\\bcare package") then 1 else 0 end) as carepackage
  , max(case when regexp_contains(qm.query, "(\?i)\\bgiftbox|\\bgift box") then 1 else 0 end) as giftbox
  , max(case when regexp_contains(qm.query, "(\?i)\\bpresent") then 1 else 0 end) as present
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE 
	_date >= current_date - 30
group by all
); 

create or replace temporary table agg as (
select
  count(distinct case when a.score >= 0.71 then c.visit_id end) as visits_giftiness7
  , count(distinct case when b.regex_gift=1 then c.visit_id end) as visits_regex_gift
  , count(distinct case when b.regex_gifting_holidays_occasions=1 then c.visit_id end) as visits_regex_gifting_holidays_occasions
  , count(distinct case when greeting_card=1 or carepackage=1 or present=1 then c.visit_id end) as visits_keywords
  , count(distinct case when a.score >= 0.71 or b.regex_gift=1 or b.regex_gifting_holidays_occasions=1 or greeting_card=1 or carepackage=1 or present=1 then c.visit_id end) as visits_all
  , sum(case when a.score >= 0.71 then total_gms end) as gms_giftiness7
  , sum(case when b.regex_gift=1 then total_gms end) as gms_regex_gift
  , sum(case when b.regex_gifting_holidays_occasions=1 then total_gms end) as gms_regex_gifting_holidays_occasions
  , sum(case when greeting_card=1 or carepackage=1 or giftbox=1 or present=1 then total_gms end) as gms_keywords
  , sum(case when a.score >= 0.71 or b.regex_gift=1 or b.regex_gifting_holidays_occasions=1 or greeting_card=1 or carepackage=1 or giftbox=1 or present=1 then total_gms end) as gms_all
from 
  etsy-data-warehouse-prod.weblog.visits c
left join 
  query_giftiness a
    using (visit_id)
left join 
  keywords b
    on c.visit_id=b.visit_id
where c._date>= current_date-30
);

end
