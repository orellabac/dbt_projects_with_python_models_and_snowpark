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
  cast(spend as float)                                     as spend,
  clicks                                   as clicks,
  impressions                              as impressions,
  conversions                              as conversions,
  cast(revenue as float)                                   as revenue,
  case when impressions > 0
       then (clicks / impressions)
       else null end                                           as ctr,
  case when clicks > 0
       then spend  / clicks 
       else null end                                           as cpc,
  case when conversions > 0
       then (spend / conversions)
       else null end                                           as cpa,
  case when spend  > 0
       then (revenue  / spend )
       else null end                                           as roas,
  case
    when spend >= 10000 then 'enterprise'
    when spend >= 1000  then 'large'
    when spend >= 100   then 'medium'
    when spend is not null then 'small'
    else 'unknown'
  end                                                          as spend_bucket
from raw

