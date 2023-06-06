
-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
select
    format_date('%Y%m',parse_date('%Y%m%d', date)) as month,
    count(fullVisitorId) as visits,
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions,
    sum(totals.totalTransactionRevenue)/ power(10,6) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170101' and '20170331'
group by month
order by month;


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
select 
      trafficSource.source as source,
      count(fullVisitorId) as total_visits,
      count(totals.bounces) as total_no_of_bounces,
      round(count(totals.bounces) / count(fullVisitorId) *100,8) as bounce_rate
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by source
order by total_visits desc
limit 4;


-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
select 
      case time when '201706' then 'Month' else 'Week' end as time_type,
      *
from (
      select 
            format_date('%Y%m', parse_date('%Y%m%d',date)) as time,
            trafficSource.source as source,
            sum(totals.totalTransactionRevenue)/ power(10,6) as revenue
      from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
      group by time, source

      UNION ALL
      select 
            format_date('%Y%W', parse_date('%Y%m%d',date)) as time,
            trafficSource.source as source,
            sum(totals.totalTransactionRevenue)/ power(10,6) as revenue
      from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
      group by time, source

      order by revenue desc
      ) as union_table
limit 4;


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

with p as(
      select
      format_date('%Y%m',parse_date('%Y%m%d', date)) as month,
      count(distinct fullVisitorId) as visits,
      sum(totals.pageviews) as pageviews,
      sum(totals.pageviews) / count(distinct fullVisitorId) as avg_pageviews_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      Where _table_suffix between '20170601' and '20170731'
      and totals.transactions is not null
      group by month
      ),
    np as(
      select
      format_date('%Y%m',parse_date('%Y%m%d', date)) as month,
      count(distinct fullVisitorId) as visits,
      sum(totals.pageviews) as pageviews,
      sum(totals.pageviews) / count(distinct fullVisitorId) as avg_pageviews_non_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      Where _table_suffix between '20170601' and '20170731'
      and totals.transactions is null
      group by month
      )
select p.month,
       round(p.avg_pageviews_purchase,8),
       round(np.avg_pageviews_non_purchase,9)
from p
join np 
on p.month = np.month
order by month;


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
with s as(
        select
            format_date('%Y%m',parse_date('%Y%m%d', date)) as month,
            count(distinct fullVisitorId) as visits,
            sum(totals.transactions) as transactions,
        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
        where totals.transactions is not null
        group by month
        )
select
    s.month,
    s.transactions / s.visits as Avg_total_transactions_per_user
from s 

-- Query 06: Average amount of money spent per session
#standardSQL
with s as (
select
    format_date('%Y%m',parse_date('%Y%m%d', date)) as month,
    count(fullVisitorId) as visits,
    sum(totals.totalTransactionRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions is not null
group by month
)
select
    s.month,
    s.revenue / s.visits as avg_revenue_by_user_per_visit
from s 


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
with a as (
        select 
              fullVisitorId,
              v2ProductName,
              productQuantity
        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` as main, unnest(hits) as hits, unnest(hits.product) as product
        where product.productRevenue is not null
        ),
    y as (
        select distinct fullVisitorId
        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` as main, unnest(hits) as hits, unnest(hits.product) as product
        where v2ProductName = "YouTube Men's Vintage Henley" 
        and product.productRevenue is not null)
select 
      a.v2ProductName as other_purchased_products,
      sum(a.productQuantity) as quantity
from a 
join y on a.fullVisitorId = y.fullVisitorId
where v2ProductName <> "YouTube Men's Vintage Henley"
group by v2ProductName
order by quantity desc;


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with s as (
        select
              format_date('%Y%m',parse_date('%Y%m%d', date)) as month,
              sum(case when hits.eCommerceAction.action_type = "2" then 1 end) as num_product_view,
              sum(case when hits.eCommerceAction.action_type = "3" then 1 end) as num_addtocart,
              sum(case when hits.eCommerceAction.action_type = "6" then 1 end) as num_purchase
        from `bigquery-public-data.google_analytics_sample.ga_sessions_*`, unnest(hits) as hits, unnest(hits.product) as product
        where _table_suffix between '20170101' and '20170331'
        group by month
        order by month
        )
select 
      s.month,
      s.num_product_view,
      s.num_addtocart,
      s.num_purchase,
      round(s.num_addtocart/s.num_product_view *100,2) as add_to_cart_rate,
      round(s.num_purchase/s.num_product_view *100,2) as purchase_rate
from s 
group by s.month, s.num_product_view,s.num_addtocart,s.num_purchase
order by s.month;
