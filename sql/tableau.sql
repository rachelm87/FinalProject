-- Table 1: All rDROP VIEW IF EXISTS v_tableau_space_records;

CREATE VIEW v_tableau_space_records AS
WITH gpi_latest AS (
  SELECT DISTINCT ON (country)
         country,
         gpi_rank,
         gpi_score
  FROM peace_index
  ORDER BY country, year DESC
)
SELECT
  r.event_id,
  r.published_date,
  r.time_classified,
  r.title,

  CASE
    WHEN r.summary IS NULL
      OR btrim(r.summary) = ''
      OR r.summary IN ('[]', '{}')
      OR (r.summary ILIKE '%news.google.com/rss/articles%' AND r.summary ILIKE '%<a href=%')
    THEN 'No summary available'
    ELSE r.summary
  END AS summary,

  r.source,
  r.source_api,
  r.event_type,
  r.is_space_related,
  r.is_security_related,
  r.countries,
  r.entities,

  g.countries_gpi,

  h.story_cluster_id AS ml_story_cluster_id,
  h.rep_title        AS ml_rep_title,
  h.article_count    AS ml_article_count

FROM space_records r

LEFT JOIN LATERAL (
  SELECT
    COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'country', d.country,
          'gpi_rank', gl.gpi_rank,
          'gpi_score', gl.gpi_score
        )
        ORDER BY (gl.gpi_rank IS NULL), gl.gpi_rank, d.country
      ),
      '[]'::jsonb
    ) AS countries_gpi
  FROM (
    SELECT DISTINCT unnest(COALESCE(r.countries, ARRAY[]::text[])) AS country
  ) d
  LEFT JOIN gpi_latest gl
    ON gl.country = CASE
      WHEN d.country IN ('United States', 'US', 'U.S.', 'USA') THEN 'United States of America'
      WHEN d.country IN ('Turkey', 'Türkiye', 'T&uuml;rkiye') THEN 'Turkiye'
      ELSE d.country
    END
) g ON TRUE

LEFT JOIN LATERAL (
  SELECT h.story_cluster_id, h.rep_title, h.article_count
  FROM v_space_headlines_period h
  WHERE h.rep_url = r.raw_source
     OR (lower(h.rep_title) = lower(r.title) AND h.source = r.source)
  ORDER BY (h.rep_url = r.raw_source) DESC, h.published_max DESC
  LIMIT 1
) h ON TRUE

WHERE COALESCE(cardinality(r.countries), 0) > 0
   OR COALESCE(cardinality(r.entities), 0) > 0;

-- Table 2: Unnests the countries/entities along with corresponding peace rank for analysis
DROP VIEW IF EXISTS v_space_mentions;

CREATE VIEW v_space_mentions AS
WITH gpi_latest AS (
  -- Latest available row per country (avoids nulls if some countries miss the global max year)
  SELECT DISTINCT ON (country)
         country,
         gpi_rank,
         gpi_score
  FROM peace_index
  ORDER BY country, year DESC
),

country_mentions AS (
  SELECT
    r.event_id,
    r.published_date,
    r.source,
    r.source_api,
    r.event_type,
    r.is_security_related,

    'country'::text AS mention_type,
    c.country       AS mention_value,

    gl.gpi_rank,
    gl.gpi_score
  FROM space_records r
  CROSS JOIN LATERAL (
    SELECT DISTINCT unnest(COALESCE(r.countries, ARRAY[]::text[])) AS country
  ) c
  LEFT JOIN gpi_latest gl
    ON gl.country = CASE
      WHEN c.country IN ('United States', 'US', 'U.S.', 'USA') THEN 'United States of America'
      WHEN c.country IN ('Turkey', 'Türkiye', 'T&uuml;rkiye') THEN 'Turkiye'
      ELSE c.country
    END
  WHERE c.country IS NOT NULL
    AND btrim(c.country) <> ''
),

entity_mentions AS (
  SELECT
    r.event_id,
    r.published_date,
    r.source,
    r.source_api,
    r.event_type,
    r.is_security_related,

    'entity'::text AS mention_type,
    e.entity        AS mention_value,

    NULL::int       AS gpi_rank,
    NULL::numeric   AS gpi_score
  FROM space_records r
  CROSS JOIN LATERAL (
    SELECT DISTINCT unnest(COALESCE(r.entities, ARRAY[]::text[])) AS entity
  ) e
  WHERE e.entity IS NOT NULL
    AND btrim(e.entity) <> ''
)

SELECT * FROM country_mentions
UNION ALL
SELECT * FROM entity_mentions;
