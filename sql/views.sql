/*
views.sql supports analysis and subsequent visualization via Tableau
*/

------------------------------------------------------------
-- 1. INITIAL BASELINE VIEWS
------------------------------------------------------------

-- Baseline: security-related records
CREATE OR REPLACE VIEW v_baseline_filter AS
SELECT
    sr.event_id,
    sr.published_date,
    sr.title,
    sr.summary,
    sr.event_type,
    sr.countries,
    sr.entities
FROM space_records sr
WHERE sr.is_security_related = TRUE
  AND NOT (
    sr.countries = '{}'
    AND sr.entities = '{}'
  );

-- STRICT Baseline: space+security-related records
CREATE OR REPLACE VIEW v_baseline_space_security AS
SELECT
    sr.event_id,
    sr.published_date,
    sr.title,
    sr.summary,
    sr.event_type,
    sr.countries,
    sr.entities,
    sr.source,
    sr.source_api
FROM space_records sr
WHERE sr.is_space_related = TRUE
  AND sr.event_type = 'security_event'
  AND NOT (
    sr.countries = '{}'
    AND sr.entities = '{}'
  );

------------------------------------------------------------
-- 2. CONSOLIDATION: COUNTRY / ENTITY
------------------------------------------------------------

-- Security events by country
CREATE OR REPLACE VIEW v_security_events_by_country AS
SELECT
    c.country,
    COUNT(*) AS event_count
FROM v_baseline_filter bf
CROSS JOIN LATERAL unnest(bf.countries) AS c(country)
GROUP BY c.country;

-- Security events by entity
CREATE OR REPLACE VIEW v_security_events_by_entity AS
SELECT
    e.entity,
    COUNT(*) AS event_count
FROM v_baseline_filter bf
CROSS JOIN LATERAL unnest(bf.entities) AS e(entity)
GROUP BY e.entity;

------------------------------------------------------------
-- 3. TIME OVER TIME TRENDS
------------------------------------------------------------

-- Weekly security trends by country
CREATE OR REPLACE VIEW v_weekly_country_trends AS
SELECT
    date_trunc('week', bf.published_date) AS week,
    c.country,
    COUNT(*) AS event_count
FROM v_baseline_filter bf
CROSS JOIN LATERAL unnest(bf.countries) AS c(country)
GROUP BY week, c.country;

-- Weekly security trends by entity
CREATE OR REPLACE VIEW v_weekly_entity_trends AS
SELECT
    date_trunc('week', bf.published_date) AS week,
    e.entity,
    COUNT(*) AS event_count
FROM v_baseline_filter bf
CROSS JOIN LATERAL unnest(bf.entities) AS e(entity)
GROUP BY week, e.entity;

------------------------------------------------------------
-- 4. WHO IS DOING WHAT WITH WHOM/WHERE
------------------------------------------------------------

-- Countries working together (joint appearance in events)
CREATE OR REPLACE VIEW v_country_cooccurrence AS
SELECT
    c1.country AS country_a,
    c2.country AS country_b,
    COUNT(*) AS co_occurrence_count
FROM v_baseline_filter bf
CROSS JOIN LATERAL unnest(bf.countries) AS c1(country)
CROSS JOIN LATERAL unnest(bf.countries) AS c2(country)
WHERE c1.country < c2.country
GROUP BY country_a, country_b;

-- Entity co-occurrence
CREATE OR REPLACE VIEW v_entity_cooccurrence AS
SELECT
    e1.entity AS entity_a,
    e2.entity AS entity_b,
    COUNT(*) AS co_occurrence_count
FROM v_baseline_filter bf
CROSS JOIN LATERAL unnest(bf.entities) AS e1(entity)
CROSS JOIN LATERAL unnest(bf.entities) AS e2(entity)
WHERE e1.entity < e2.entity
GROUP BY entity_a, entity_b;

-- Country–entity shared records
CREATE OR REPLACE VIEW v_country_entity_shared AS
SELECT
    c.country,
    e.entity,
    COUNT(*) AS shared_event_count
FROM v_baseline_filter bf
CROSS JOIN LATERAL unnest(bf.countries) AS c(country)
CROSS JOIN LATERAL unnest(bf.entities) AS e(entity)
GROUP BY c.country, e.entity;

------------------------------------------------------------
-- 5. MACHINE-LEARNING HEADLINES
------------------------------------------------------------

CREATE OR REPLACE VIEW v_space_headlines_period AS
SELECT
    story_cluster_id,
    published_max,
    source,
    rep_title,
    rep_url,
    event_type AS event_type_mode,
    is_security_related,
    article_count
FROM space_headlines_period;
