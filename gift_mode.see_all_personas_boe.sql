-------------------------------------------------------------------------------------------
GET BROWSERS IN EXPERIMENT
-------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING DEFAULT "gift_mode.see_all_personas_boe";
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";
DECLARE is_event_filtered BOOL; -- DEFAULT FALSE;
DECLARE bucketing_id_type INT64;

IF start_date IS NULL OR end_date IS NULL THEN
    SET (start_date, end_date) = (
        SELECT AS STRUCT
            MAX(DATE(boundary_start_ts)) AS start_date,
            MAX(_date) AS end_date,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            experiment_id = config_flag_param
    );
END IF;

IF is_event_filtered IS NULL THEN
    SET (is_event_filtered, bucketing_id_type) = (
        SELECT AS STRUCT
            is_filtered,
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
ELSE
    SET bucketing_id_type = (
        SELECT
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
END IF;

-- Get the first bucketing moment for each experimental unit (e.g. browser or user).
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS (
    SELECT
        bucketing_id,
        bucketing_id_type AS bucketing_id_type,
        variant_id,
        MIN(bucketing_ts) AS bucketing_ts,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.bucketing`
    WHERE
        _date BETWEEN start_date AND end_date
        AND experiment_id = config_flag_param
    GROUP BY
        bucketing_id, bucketing_id_type, variant_id
);

-- Need visit ids to join with non-Catapult tables?
-- No problem! Here are some examples for how to get the visit ids for each experimental unit.
-- All associated IDs in the bucketing visit
-- IF NOT is_event_filtered THEN
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            a.bucketing_ts,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 4) AS sequence_number,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 1) AS browser_id,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 2) AS user_id,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 3) AS visit_id,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.bucketing` b
            USING(bucketing_id, variant_id, bucketing_ts)
        WHERE
            b._date BETWEEN start_date AND end_date
            AND b.experiment_id = config_flag_param
    );

-------------------------------------------------------------------------------------------
PERSONA CLICK RATE
-------------------------------------------------------------------------------------------
--click rate from personas
with agg as (
select
  a.visit_id
  , a.bucketing_id
  , a.variant_id
  , b.event_type
  , lead(b.event_type) over (partition by b.visit_id order by b.sequence_number) as next_page
from 
  `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket`a
inner join 
  etsy-data-warehouse-prod.weblog.events b 
    using (visit_id)
where 
  b.page_view=1
)
select
count(distinct bucketing_id) as total_browers
  , count(distinct case when variant_id in ('on') then bucketing_id end) as treament_total_browers
  , count(distinct case when variant_id in ('off') then bucketing_id end) as control_total_browers
--treatment
  , count(distinct case when variant_id in ('on') and event_type in ('gift_mode_see_all_personas') then bucketing_id end) as treatment_browsers_with_see_all
  , count(distinct case when variant_id in ('on') and event_type in ('gift_mode_see_all_personas') and next_page in ('gift_mode_persona') then bucketing_id end) as treatment_browsers_persona_view
--control
    , count(distinct case when variant_id in ('off') and event_type in ('gift_mode_quiz_results') then bucketing_id end) as control_browsers_quiz_results
  , count(distinct case when variant_id in ('off') and event_type in ('gift_mode_quiz_results') and next_page in ('gift_mode_persona') then bucketing_id end) as control_browsers_persona_view
from agg
    
-------------------------------------------------------------------------------------------
WHERE SEE ALL PERSONA CLICKS COME FROM 
-------------------------------------------------------------------------------------------
--click rate from personas
with agg as (
select
  a.visit_id
  , a.bucketing_id
  , a.variant_id
  , b.event_type
  , lead(b.event_type) over (partition by b.visit_id order by b.sequence_number) as next_page
from 
  `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket`a
inner join 
  etsy-data-warehouse-prod.weblog.events b 
    using (visit_id)
where variant_id in ('on') and event_type in ('gift_mode_see_all_personas','gift_mode_popular_personas_browse_all_tapped','gift_mode_header_collapsed_browse_all_personas_tapped')
)
select
count(distinct bucketing_id) as total_browers
, count(distinct case when event_type in ('gift_mode_see_all_personas') then bucketing_id end) as browsers_see_all_personas
, count(distinct case when event_type in ('gift_mode_popular_personas_browse_all_tapped') then bucketing_id end) as carousel_tap_event
, count(distinct case when event_type in ('gift_mode_popular_personas_browse_all_tapped') and next_page in ('gift_mode_see_all_personas') then bucketing_id end) as carousel_tap
, count(distinct case when event_type in ('gift_mode_header_collapsed_browse_all_personas_tapped') then bucketing_id end) as sticky_tap_event
, count(distinct case when event_type in ('gift_mode_header_collapsed_browse_all_personas_tapped') and next_page in ('gift_mode_see_all_personas') then bucketing_id end) as sticky_button_tap
from agg
    
-------------------------------------------------------------------------------------------
BROWSER JOURNEY
-------------------------------------------------------------------------------------------
with events as (
select
  a.visit_id
  , a.bucketing_id
  , a.variant_id
  , b.event_type
  , b.sequence_number
  , lead(b.event_type) over (partition by b.visit_id order by b.sequence_number) as next_page
from 
  `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
inner join 
  etsy-data-warehouse-prod.weblog.events b 
    using (visit_id)
where 
  page_view =1
)
, events_sub as ( --get events for each bucketing_id post event
select
variant_id
  , bucketing_id
  , count(event_type) pages_viewed  
  , count(case when event_type like ('gift_mode%') and next_page is null then event_type end) as gift_mode_exit
  , count(case when event_type in ('gift_mode_persona') and next_page is null then event_type end) as gift_mode_persona_exit
  , count(case when event_type like ('gift_mode%') then event_type end) as gift_mode_pages 
  , count(case when event_type in ('gift_mode_persona') then event_type end) as persona_pages
from events 
where
  variant_id in ('on')
  and sequence_number > (select min(sequence_number) from events where event_type in ('gift_mode_see_all_personas')) --everything after see all
group by all 
union all 
select
variant_id
  , bucketing_id
  , count(event_type) pages_viewed  
  , count(case when event_type like ('gift_mode%') and next_page is null then event_type end) as gift_mode_exit
  , count(case when event_type in ('gift_mode_persona') and next_page is null then event_type end) as gift_mode_persona_exit
  , count(case when event_type like ('gift_mode%') then event_type end) as gift_mode_pages 
  , count(case when event_type in ('gift_mode_persona') then event_type end) as persona_pages
from events 
where
  variant_id in ('off')
  and sequence_number > (select min(sequence_number) from events where event_type in ('gift_mode_quiz_results')) --everything after see all
group by all 
)
select
variant_id
  , count(distinct bucketing_id) as browsers
  , sum(pages_viewed)/count(distinct bucketing_id) as avg_pages_viewed
  , sum(gift_mode_persona_exit)/ count(distinct bucketing_id) as persona_exit_rate
  , sum(gift_mode_exit)/ count(distinct bucketing_id) as gm_exit_rate
  , sum(gift_mode_pages)/count(distinct bucketing_id) as avg_gm_pages_viewed
  , sum(persona_pages)/count(distinct bucketing_id) as avg_persona_pages_viewed
from 
  events_sub
group by all 

 

