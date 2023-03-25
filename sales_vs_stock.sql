SELECT
  *
FROM (
  SELECT
    *
  FROM (
    SELECT
      date,
      brand AS Brand,
      ASIN,
      IFNULL(product_group,'') AS Product_group,
      ROUND(SUM(IFNULL(net_sale_unit,0)),2) AS Net_sale_unit,
      round(sum(ifnull(net_sale,0)),2) AS Gross_sales,
      ROUND(SUM(IFNULL(net_sale,0) + IFNULL(promotion,0) + IFNULL(shipping_revenue,0) + IFNULL(RETURNS,0)),2) AS Net_sale_euro,
      ROUND(SUM(IFNULL(commission,0) + IFNULL(storage_cost,0) + IFNULL(transport_cost,0) + IFNULL(fba_fees,0) + IFNULL(purchase_price,0) + IFNULL(ad_cost,0)),2) AS All_costs_euro,
      ROUND(SUM(IFNULL(commission,0) + IFNULL(storage_cost,0) + IFNULL(transport_cost,0) + IFNULL(fba_fees,0) + IFNULL(purchase_price,0) + IFNULL(ad_cost,0) + IFNULL(net_sale,0) + IFNULL(promotion,0) + IFNULL(shipping_revenue,0) + IFNULL(RETURNS,0)),2) AS CM2_euro,
      ROUND(SUM(IFNULL(commission,0) + IFNULL(storage_cost,0) + IFNULL(transport_cost,0) + IFNULL(fba_fees,0) + IFNULL(purchase_price,0)),2) AS All_costs_ex_ad_euro,
      ROUND(SUM(IFNULL(purchase_price,0)),2) AS COGS_euro,
      ROUND(SUM(IFNULL(commission,0)),2) AS Commission_euro,
      ROUND(SUM(IFNULL(storage_cost,0)),2) AS Storage_cost_euro,
      ROUND(SUM(IFNULL(transport_cost,0)),2) AS Transport_cost_euro,
      ROUND(SUM(IFNULL(fba_fees,0)),2) AS FBA_fee_euro,
      ROUND(SUM(promotion),2) AS Promotion_euro,
      ROUND(SUM(IFNULL(shipping_revenue,0)),2) AS Shipping_revenue_euro,
      ROUND(SUM(IFNULL(RETURNS,0)),2) AS RETURNS_euro,
      ROUND(SUM(IFNULL(ad_cost,0)),2) AS Ad_cost_euro,
      ROUND(SUM(IFNULL(ad_cost,0) *(-1)),2) AS Ad_cost_positive_euro,
      ROUND(AVG(rating),1) AS Rating,
    FROM
      `stryze-data-gcp.dbt_prod.mart_order_items_product`
    WHERE
      platform = 'amazon'
      AND date >= '2022-12-01'
    GROUP BY
      date,
      brand,
      ASIN,
      product_group ) l
  JOIN (
    SELECT
      asin AS m_asin,
      SUM(IFNULL(net_sale_unit,0)) AS Units_sold_30_day,
      ROUND(SUM(IFNULL(net_sale_unit,0)) / 30,1) AS Average_sale_1_day,
      ROUND(SUM(IFNULL(net_sale,0)) / 30,2) AS Average_gross_sale_euro_1_day
    FROM
      `stryze-data-gcp.dbt_prod.mart_order_items_product`
    WHERE
      platform = 'amazon'
      AND Date >= DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY)
    GROUP BY
      asin ) m
  ON
    l.asin = m.m_asin ) s
LEFT JOIN (
  SELECT
    date AS Date_stock,
    asin AS ASIN_stock,
    ROUND(SUM(count),0) AS Stryze_stock
  FROM
    `dbt_prod.mart_sp_stock_products_view`
  WHERE
    type <> 'amazon reserved transfers'
    AND type <> 'amazon inbound shipped'
    AND type <> 'amazon inbound receiving'
    AND type <> 'amazon unsellable'
    AND type <> 'amazon inbound working'
  GROUP BY
    date,
    asin ) r
ON
  r.ASIN_stock= s.asin
  AND r.Date_stock = s.date
ORDER BY
  s.date DESC,
  s.asin

  limit 10
