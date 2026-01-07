WITH gpi_latest AS (
  SELECT country, gpi_rank, gpi_score
  FROM peace_index
  WHERE year = (SELECT MAX(year) FROM peace_index)
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

  h.story_cluster_id     AS ml_story_cluster_id,
  h.published_max        AS ml_published_max,
  h.rep_title            AS ml_rep_title,
  h.rep_url              AS ml_rep_url,
  h.event_type_mode      AS ml_event_type,
  h.is_security_related  AS ml_is_security_related,
  h.article_count        AS ml_article_count

FROM space_records r

LEFT JOIN LATERAL (
  SELECT jsonb_agg(
           jsonb_build_object(
             'country', d.country,
             'gpi_rank', gl.gpi_rank,
             'gpi_score', gl.gpi_score
           )
           ORDER BY (gl.gpi_rank IS NULL), gl.gpi_rank, d.country
         ) AS countries_gpi
  FROM (
    SELECT DISTINCT unnest(r.countries) AS country
  ) d
  LEFT JOIN gpi_latest gl
    ON gl.country = d.country
) g ON TRUE

LEFT JOIN v_space_headlines_period h
  ON (
       r.raw_source = h.rep_url
       OR (
            lower(r.title) = lower(h.rep_title)
            AND r.source = h.source
          )
     )

WHERE cardinality(r.countries) > 0
   OR cardinality(r.entities) > 0

ORDER BY r.published_date DESC;
