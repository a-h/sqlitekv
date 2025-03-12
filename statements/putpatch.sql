with input_data as (
  select
    json_extract(value, '$.key') as key,
    json_extract(value, '$.version') as version,
    json_extract(value, '$.value') as value,
    json_extract(value, '$.operation') as operation
  from json_each(:input_data)
),
valid_ops as (
  select
      input_data.key,
      input_data.version,
      input_data.value,
      input_data.operation,
      kv.version as existing_version
  from 
    input_data
  left join kv on 
    kv.key = input_data.key
  where
    (input_data.version = -1 or kv.version = input_data.version) and (input_data.version <> 0)
)
insert into kv (key, version, value, created)
select
  valid_ops.key,
  1,
  jsonb(valid_ops.value),
  :now
from valid_ops
where 
  (select count(*) from input_data) = (select count(*) from valid_ops)
on conflict(key) do update
set
  version = kv.version + 1,
  value = case
    when (select count(*) from valid_ops vo where vo.key = kv.key and vo.operation = 'patch') = 0
      then jsonb_patch(value, excluded.value)
      else jsonb(excluded.value)
    end
where 
  (select count(*) from input_data) = (select count(*) from valid_ops)
