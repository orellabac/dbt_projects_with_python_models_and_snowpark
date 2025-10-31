-- Intermediate model computing KPIs from the staging Google Sheets data

with raw as (
  select
    DATE,
    CHANNEL,
    SPEND,
    CLICKS,
    IMPRESSIONS,
    CONVERSIONS,
    REVENUE
  from {{ ref('stg_google_sheets_campaigns') }}
)

select
  try_cast(date as date)                                       as date,
  channel,
  try_cast(spend as float)                                     as spend,
  try_cast(clicks as number)                                   as clicks,
  try_cast(impressions as number)                              as impressions,
  try_cast(conversions as number)                              as conversions,
  try_cast(revenue as float)                                   as revenue,
  case when try_cast(impressions as number) > 0
       then (try_cast(clicks as number) / try_cast(impressions as number))
       else null end                                           as ctr,
  case when try_cast(clicks as number) > 0
       then (try_cast(spend as float) / try_cast(clicks as number))
       else null end                                           as cpc,
  case when try_cast(conversions as number) > 0
       then (try_cast(spend as float) / try_cast(conversions as number))
       else null end                                           as cpa,
  case when try_cast(spend as float) > 0
       then (try_cast(revenue as float) / try_cast(spend as float))
       else null end                                           as roas,
  case
    when try_cast(spend as float) >= 10000 then 'enterprise'
    when try_cast(spend as float) >= 1000  then 'large'
    when try_cast(spend as float) >= 100   then 'medium'
    when try_cast(spend as float) is not null then 'small'
    else 'unknown'
  end                                                          as spend_bucket
from raw

