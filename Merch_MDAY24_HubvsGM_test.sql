-------------------------------------------------------------------------------------------
-- INPUT
-------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING DEFAULT "dynamic_experiments.Merch_MDAY24_HubvsGM_test";
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

-- For event filtered experiments, the effective bucketing event for a bucketed unit
-- into a variant is the FIRST filtering event to occur after that bucketed unit was
-- bucketed into that variant of the experiment.
IF is_event_filtered THEN
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            MIN(f.event_ts) AS bucketing_ts,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
            USING(bucketing_id)
        WHERE
            f._date BETWEEN start_date AND end_date
            AND f.experiment_id = config_flag_param
            AND f.event_ts >= f.boundary_start_ts
            AND f.event_ts >= a.bucketing_ts
        GROUP BY
            bucketing_id, bucketing_id_type, variant_id
    );
END IF;

-------------------------------------------------------------------------------------------
-- SEGMENT DATA
-------------------------------------------------------------------------------------------
-- Get segment values based on first bucketing moment.
-- Example output:
-- bucketing_id | variant_id | event_id         | event_value
-- 123          | off        | buyer_segment    | New
-- 123          | off        | canonical_region | FR
-- 456          | on         | buyer_segment    | Habitual
-- 456          | on         | canonical_region | US
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.first_bucket_segments_unpivoted` AS (
    SELECT
        a.bucketing_id,
        a.variant_id,
        s.event_id,
        s.event_value,
    FROM
        `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
    JOIN
        `etsy-data-warehouse-prod.catapult_unified.segment_event` s
        USING(bucketing_id, bucketing_ts)
    WHERE
        s._date BETWEEN start_date AND end_date
        AND s.experiment_id = config_flag_param
        -- <SEGMENTATION> Here you can specify whatever segmentations you'd like to analyze.
        -- !!! Please keep this in sync with the PIVOT statement below !!!
        -- For all supported segmentations, see go/catapult-unified-docs.
        AND s.event_id IN (
            "buyer_segment",
            "canonical_region"
        )
);

-- Pivot the above table to get one row per bucketing_id and variant_id. Each additional
-- column will be a different segmentation, and the value will be the segment for each
-- bucketing_id at the time they were first bucketed into the experiment date range being
-- analyzed.
-- Example output (using the same example data above):
-- bucketing_id | variant_id | buyer_segment | canonical_region
-- 123          | off        | New           | FR
-- 456          | on         | Habitual      | US
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.first_bucket_segments` AS (
    SELECT
        *
    FROM
        `etsy-data-warehouse-dev.madelinecollins.first_bucket_segments_unpivoted`
    PIVOT(
        MAX(event_value)
        FOR event_id IN (
            "buyer_segment",
            "canonical_region"
        )
    )
);

-------------------------------------------------------------------------------------------
-- EVENT AND GMS DATA
-------------------------------------------------------------------------------------------
-- <EVENT> Specify the events you want to analyze here.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.events` AS (
    SELECT
        *
    FROM
        UNNEST([
            "backend_cart_payment", -- conversion rate
            "total_winsorized_gms", -- winsorized acbv
            "prolist_total_spend",  -- prolist revenue
            "gms",    -- note: gms data is in cents
            "bounce",
            "backend_add_to_cart",
            "checkout_start",
            "offsite_ads_one_day_attributed_revenue" ,
            "total_winsorized_order_value",
            "visits"
        ]) AS event_id
);

-- Get all the bucketed units with the events of interest.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.events_per_unit` AS (
    SELECT
        a.bucketing_id,
        a.variant_id,
        e.event_id,
        CAST(SUM(e.event_value) AS FLOAT64) AS event_value,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.event` e
    CROSS JOIN
        UNNEST(e.associated_ids) ids
    JOIN
        `etsy-data-warehouse-dev.madelinecollins.events`
        USING(event_id)
    JOIN
        `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
        ON a.bucketing_id = ids.id
        AND a.bucketing_id_type = ids.id_type
    WHERE
        e._date BETWEEN start_date AND end_date
        AND e.event_type IN (1, 3, 4) -- fired, gms, and bounce events (see go/catapult-unified-enums)
        AND e.event_ts >= a.bucketing_ts
    GROUP BY
        bucketing_id, variant_id, event_id
);

-- Insert custom events separately, as custom event data does not exist in the event table (as of Q4 2023).
IF bucketing_id_type = 1 THEN -- browser data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.post_bucketing_custom_events` AS (
        WITH custom_events AS (
            SELECT
                a.bucketing_id,
                v.visit_id,
                a.variant_id,
                a.bucketing_ts,
                v.sequence_number,
                v.event_name AS event_id,
                v.event_data AS event_value,
                v.event_timestamp,
            FROM
                `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
            JOIN
                `etsy-data-warehouse-prod.catapult.visit_segment_custom_metrics` v
                ON a.bucketing_id = SPLIT(v.visit_id, '.')[OFFSET(0)]
            WHERE
                v._date BETWEEN start_date AND end_date
                AND v.event_timestamp >= a.bucketing_ts
                and v.event_name in ('total_winsorized_gms','total_winsorized_order_value','prolist_total_spend','offsite_ads_one_day_attributed_revenue','visits')
        )
        SELECT
            bucketing_id,
            visit_id,
            variant_id,
            bucketing_ts,
            sequence_number,
            event_id,
            event_value,
            ROW_NUMBER() OVER (
                PARTITION BY bucketing_id, variant_id, event_id
                ORDER BY event_timestamp, visit_id, sequence_number
            ) AS row_number,
        FROM
            custom_events
    );

    INSERT INTO `etsy-data-warehouse-dev.madelinecollins.events_per_unit` (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            SUM(event_value) AS event_value,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.post_bucketing_custom_events`
        WHERE
            row_number = 1
            OR (row_number > 1 AND sequence_number = 0)
        GROUP BY
            bucketing_id, variant_id, event_id
    );
ELSEIF bucketing_id_type = 2 THEN -- user data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.post_bucketing_custom_events` AS (
        WITH custom_events AS (
            SELECT
                a.bucketing_id,
                c.visit_id,
                a.variant_id,
                a.bucketing_ts,
                c.sequence_number,
                c.event_name AS event_id,
                c.event_data AS event_value,
                c.event_timestamp,
            FROM
                `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
            JOIN
                `etsy-data-warehouse-prod.catapult.custom_events_by_user_slice` c
                ON a.bucketing_id = c.user_id
            WHERE
                c._date BETWEEN start_date AND end_date
                AND c.event_timestamp >= a.bucketing_ts
        )
        SELECT
            bucketing_id,
            visit_id,
            variant_id,
            bucketing_ts,
            sequence_number,
            event_id,
            event_value,
            ROW_NUMBER() OVER (
                PARTITION BY bucketing_id, variant_id, event_id
                ORDER BY event_timestamp, visit_id, sequence_number
            ) AS row_number,
            ROW_NUMBER() OVER (
                PARTITION BY bucketing_id, variant_id, event_id, visit_id
                ORDER BY sequence_number
            ) AS row_number_in_visit,
        FROM
            custom_events
    );

    INSERT INTO `etsy-data-warehouse-dev.madelinecollins.events_per_unit` (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            SUM(event_value) AS event_value,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.post_bucketing_custom_events`
        WHERE
            row_number = 1
            OR (row_number > 1 AND row_number_in_visit = 1)
        GROUP BY
            bucketing_id, variant_id, event_id
    );
END IF;
-------------------------------------------------------------------------------------------
-- COMBINE BUCKETING, EVENT & SEGMENT DATA
-------------------------------------------------------------------------------------------
-- All events for all bucketed units, with segment values.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket_clicks` AS (
select 
  a.bucketing_id,
  a.variant_id
from 
  `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
inner join 
  etsy-data-warehouse-prod.weblog.visits b
    on b.browser_id=a.bucketing_id
inner join 
  etsy-data-warehouse-prod.weblog.events c
    on b.visit_id=c.visit_id
where 
  ref_tag in ('hp_bubbles_MDAY24')
  and b._date >= current_date-30
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_segments` AS (
    SELECT
        bucketing_id,
        variant_id,
        event_id,
        COALESCE(event_value, 0) AS event_count,
        -- buyer_segment,
        -- canonical_region,
    FROM
        `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket_clicks`
    CROSS JOIN
        `etsy-data-warehouse-dev.madelinecollins.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.madelinecollins.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
    -- JOIN
    --     `etsy-data-warehouse-dev.madelinecollins.first_bucket_segments`
    --     USING(bucketing_id, variant_id)
);

-------------------------------------------------------------------------------------------
-- RECREATE CATAPULT RESULTS
-------------------------------------------------------------------------------------------
-- Proportion and mean metrics by variant and event_name
SELECT
    event_id,
    variant_id,
    count(distinct bucketing_id) as unique_browsers,
    COUNT(*) AS total_units_in_variant,
    AVG(IF(event_count = 0, 0, 1)) AS percent_units_with_event,
    AVG(event_count) AS avg_events_per_unit,
    AVG(IF(event_count = 0, NULL, event_count)) AS avg_events_per_unit_with_event
FROM
    `etsy-data-warehouse-dev.madelinecollins.all_units_events_segments`
GROUP BY
    event_id, variant_id
ORDER BY
    event_id, variant_id;

-------------------------------------------------------------------------------------------
-- RECREATE CATAPULT RESULTS : browser level so can find stat sig of means 
-------------------------------------------------------------------------------------------
-- Proportion and mean metrics by variant and event_name
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level` AS (
SELECT
    event_id,
    variant_id,
    bucketing_id, 
    event_count,
FROM
    `etsy-data-warehouse-dev.madelinecollins.all_units_events_segments`
GROUP BY
    all
ORDER BY
    event_id, variant_id
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_acbv` AS (
  select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level` where event_id in ('total_winsorized_gms')
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_gms` AS (
  select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level`where event_id in ('gms')
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_offsite_ads` AS (
  select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level` where event_id in ('offsite_ads_one_day_attributed_revenue')
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_prolist` AS (
  select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level` where event_id in ('prolist_total_spend')
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_order_value` AS (
  select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level` where event_id in ('total_winsorized_order_value')
);

-------------------------------------------------------------------------------------------
R STAT SIG 
-------------------------------------------------------------------------------------------
## acbv
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_acbv`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=1000) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## gms
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_gms`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=500) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## offsite ads
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_offsite_ads`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=500) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## prolist
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_prolist`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=500) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## order value
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_order_value`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=500) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)
control_f <- df[df$ab_variant == "off", ]

t.test(treat_f$event_count, control_f$event_count)

    
-------------------------------------------------------------------------------------------
CALC MEANS FOR METRICS MANUALLY 
-------------------------------------------------------------------------------------------
SELECT
    variant_id,
    sum(case when event_id in ('total_winsorized_order_value') then event_count end)/ sum(case when event_id in ('backend_cart_payment') then event_count end) as Winsorized_aov,
    sum(case when event_id in ('total_winsorized_gms') then event_count end)/ count(case when event_id in ('total_winsorized_gms') and event_count != 0 then event_id end) as Winsorized_acbv, 

FROM
    `etsy-data-warehouse-dev.madelinecollins.all_units_events_segments`
group by all
