WITH input_data AS (
  SELECT
    elem.value->>'key' AS key,
    (elem.value->>'version')::int AS version,
    elem.value->'value' AS value,
    elem.value->>'operation' AS operation
  FROM jsonb_array_elements(@input_data::jsonb) AS elem
),
updated_data AS (
  SELECT
    input_data.key AS key,
    COALESCE(existing_data.version, 0) + 1 AS version,
    CASE
      WHEN input_data.operation = 'patch'
        THEN jsonb_patch(COALESCE(existing_data.value, '{}'::jsonb), input_data.value)
      ELSE input_data.value
    END AS value,
    COALESCE(existing_data.created, @now) AS created
  FROM input_data
  LEFT JOIN kv AS existing_data ON input_data.key = existing_data.key
  WHERE (input_data.version = -1 OR existing_data.version = input_data.version)
    OR (input_data.version = 0 AND existing_data.version IS NULL)
)
INSERT INTO kv (key, version, value, created)
SELECT key, version, value, created FROM updated_data
WHERE (SELECT COUNT(*) FROM input_data) = (SELECT COUNT(*) FROM updated_data)
ON CONFLICT (key) DO UPDATE
SET
  version = excluded.version,
  value = excluded.value;
