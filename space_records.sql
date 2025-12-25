SELECT
    published_date,
    source_api,
    event_type,
    title,
    is_space_related,
    is_security_related
FROM space_records
ORDER BY published_date DESC
LIMIT 1000;