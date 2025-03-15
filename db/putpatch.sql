with input_data as (
  select
    json_extract(value, '$.key') as key,
    json_extract(value, '$.version') as version,
    json_extract(value, '$.value') as value,
    json_extract(value, '$.operation') as operation
  from json_each(:input_data)
),
updated_data as (
  select
      input_data.key as key,
      coalesce(existing_data.version, 0) + 1 as version,
      case
        when input_data.operation = 'patch' then jsonb_patch(coalesce(existing_data.value, '{}'), input_data.value)
        else jsonb(input_data.value)
      end as value,
      coalesce(existing_data.created, :now) as created
  from 
    input_data
  left join kv as existing_data on
    input_data.key = existing_data.key
  where
    (input_data.version = -1 or existing_data.version = input_data.version) or (input_data.version == 0 and existing_data.version is null)
)
insert into kv (key, version, value, created)
select
  key,
  version,
  value,
  created
from updated_data
where
  (select count(*) from input_data) = (select count(*) from updated_data)
on conflict(key) do update
set
  version = excluded.version,
  value = excluded.value
