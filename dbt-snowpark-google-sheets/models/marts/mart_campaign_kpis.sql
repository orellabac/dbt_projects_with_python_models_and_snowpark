-- Mart model aggregating KPIs by channel and date

with campaigns as (
  select * from {{ ref('int_google_sheets_campaigns') }}
)

select
  channel,
  date,
  sum(spend)                                        as total_spend,
  sum(clicks)                                       as total_clicks,
  sum(impressions)                                  as total_impressions,
  sum(conversions)                                  as total_conversions,
  sum(revenue)                                      as total_revenue,
  avg(ctr)                                          as avg_ctr,
  avg(cpc)                                          as avg_cpc,
  avg(cpa)                                          as avg_cpa,
  avg(roas)                                         as avg_roas,
  count(*)                                          as num_rows,
  max(spend_bucket)                                 as max_spend_bucket
from campaigns
group by channel, date
order by date desc, channel











