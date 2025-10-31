-- Aggregated mart: financial KPIs by portfolio and program

with projects as (
  select * from {{ ref('int_servicenow_projects') }}
)

select
  count(*)                                    as project_count,
  avg(case when cost is not null and cost != 0 then capex_cost / nullif(cost, 0) end) as avg_capex_pct,
  avg(case when roi is not null then roi end)            as avg_roi
from projects
-- no grouping columns selected; overall aggregate only

