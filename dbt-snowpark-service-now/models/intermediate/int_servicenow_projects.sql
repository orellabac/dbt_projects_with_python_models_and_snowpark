-- Intermediate model that reads the raw Python model 'model1', normalizes numeric columns,
-- deduplicates by project_number (keeping latest created_ts), and creates a size bucket.

with raw as (

  select
    baseline_name,
    sys_created_on,
    created_ts,
    project_number,
    portfolio,
    program,
    project_sys_id,
    cost,
    capex_cost,
    opex_cost,
    benefits,
    value,
    roi,
    discount_rate,
    npv_value,
    irr_value,
    resource_planned_cost,
    resource_allocated_cost,
    budget_cost,
    forecast_cost,
    estimate_to_completion,
    total_estimated_cost,
    capex_pct
  from {{ ref('model1') }}

)

select
  baseline_name,
  sys_created_on,
  created_ts,
  project_number,
  portfolio,
  program,
  project_sys_id,
  try_cast(cost as float)                      as cost,
  try_cast(capex_cost as float)                as capex_cost,
  try_cast(opex_cost as float)                 as opex_cost,
  try_cast(budget_cost as float)               as budget_cost,
  try_cast(forecast_cost as float)             as forecast_cost,
  try_cast(resource_planned_cost as float)     as resource_planned_cost,
  try_cast(total_estimated_cost as float)      as total_estimated_cost,
  try_cast(capex_pct as float)                 as capex_pct,
  try_cast(roi as float)                       as roi,
  -- size bucket (illustrative thresholds)
  case
    when try_cast(total_estimated_cost as float) is null then 'unknown'
    when try_cast(total_estimated_cost as float) >= 1000000 then 'enterprise'
    when try_cast(total_estimated_cost as float) >= 100000  then 'large'
    when try_cast(total_estimated_cost as float) >= 10000   then 'medium'
    else 'small'
  end as project_size
from raw
-- dedupe: keep the latest record per project_number (fallback to project_sys_id)
qualify
  case
    when project_number is not null
      then row_number() over (partition by project_number order by created_ts desc nulls last)
    else row_number() over (partition by project_sys_id order by created_ts desc nulls last)
  end = 1
;
