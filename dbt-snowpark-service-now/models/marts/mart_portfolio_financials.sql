-- Aggregated mart: financial KPIs by portfolio and program

with projects as (
  select * from {{ ref('int_servicenow_projects') }}
)

select
  coalesce(portfolio, 'UNASSIGNED') as portfolio,
  coalesce(program, 'UNASSIGNED')   as program,
  count(*)                                    as project_count,
  sum(total_estimated_cost)                   as total_estimated_cost,
  avg(case when capex_pct is not null then capex_pct end) as avg_capex_pct,
  avg(case when roi is not null then roi end)            as avg_roi,
  max(total_estimated_cost)                     as max_project_cost
from projects
group by 1, 2
order by total_estimated_cost desc
;
