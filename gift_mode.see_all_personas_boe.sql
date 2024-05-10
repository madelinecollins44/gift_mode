-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
GET BROWSERS IN EXPERIMENT
-------------------------------------------------------------------------------------------
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

-------------------------------------------------------------------------------------------
-- BUCKETING DATA
-------------------------------------------------------------------------------------------
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

-------------------------------------------------------------------------------------------
-- VISIT IDS TO JOIN WITH EXTERNAL TABLES
-------------------------------------------------------------------------------------------
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
-------------------------------------------------------------------------------------------
PERSONA CLICK RATE
-------------------------------------------------------------------------------------------
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
-------------------------------------------------------------------------------------------
WHERE SEE ALL PERSONA CLICKS COME FROM 
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
