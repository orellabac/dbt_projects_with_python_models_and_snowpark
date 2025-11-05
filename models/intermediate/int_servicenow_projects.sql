-- Intermediate model that reads the raw Python model 'model1', normalizes numeric columns,
-- deduplicates by project_number (keeping latest created_ts), and creates a size bucket.

with raw as (

  select
    BASELINE_NAME                                  as baseline_name,
    SYS_CREATED_ON                                 as sys_created_on,
    PM_PROJECT_NUMBER                              as project_number,
    --PM_PROJECT_PORTFOLIO                           as portfolio,
    --PM_PROJECT_PROGRAM                             as program,
    PM_PROJECT_SYS_ID                              as project_sys_id,
    PM_PROJECT_COST                                as cost,
    PM_PROJECT_CAPEX_COST                          as capex_cost,
    PM_PROJECT_OPEX_COST                           as opex_cost,
    PM_PROJECT_BENEFITS                            as benefits,
    PM_PROJECT_VALUE                               as value,
    PM_PROJECT_ROI                                 as roi,
    PM_PROJECT_DISCOUNT_RATE                       as discount_rate,
    PM_PROJECT_NPV_VALUE                           as npv_value,
    PM_PROJECT_IRR_VALUE                           as irr_value,
    PM_PROJECT_RESOURCE_PLANNED_COST               as resource_planned_cost,
    PM_PROJECT_RESOURCE_ALLOCATED_COST             as resource_allocated_cost,
    PM_PROJECT_BUDGET_COST                         as budget_cost,
    PM_PROJECT_FORECAST_COST                       as forecast_cost,
    PM_PROJECT_ESTIMATE_TO_COMPLETION              as estimate_to_completion
  from {{ ref('stg_servicenow_pm_project_baseline') }}

)

select
  baseline_name,
  sys_created_on,
  project_number,
  --portfolio,
  --program,
  project_sys_id,
  try_cast(cost as float)                      as cost,
  try_cast(capex_cost as float)                as capex_cost,
  try_cast(opex_cost as float)                 as opex_cost,
  try_cast(budget_cost as float)               as budget_cost,
  try_cast(forecast_cost as float)             as forecast_cost,
  try_cast(resource_planned_cost as float)     as resource_planned_cost,
  try_cast(roi as float)                       as roi,
  -- size bucket (illustrative thresholds)
  case
    when try_cast(budget_cost as float) is null then 'unknown'
    when try_cast(budget_cost as float) >= 1000000 then 'enterprise'
    when try_cast(budget_cost as float) >= 100000  then 'large'
    when try_cast(budget_cost as float) >= 10000   then 'medium'
    else 'small'
  end as project_size
from raw
-- dedupe: keep the latest record per project_number (fallback to project_sys_id)
qualify
  case
    when project_number is not null
      then row_number() over (partition by project_number order by sys_created_on desc nulls last)
    else row_number() over (partition by project_sys_id order by sys_created_on desc nulls last)
  end = 1
