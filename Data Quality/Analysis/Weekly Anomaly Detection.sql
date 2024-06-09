-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Weekly Anomaly Detection**
-- MAGIC
-- MAGIC Anomaly detection over a one-year period using three dimensions:
-- MAGIC - Vendor cost fluctuation greater than 10%
-- MAGIC - Product allocation changes (excluding new additions)
-- MAGIC - Product cost fluctuation greater than 10%

-- COMMAND ----------

-- Z score Amomaly Detection from Vendor perspective
-- Condition: 
-- 1. Within 12 months
-- 2. Cost percentage variance > 10% 
-- 3. Total Cost for the month is not available
-- 4. Z-score > 2.0 indicates an abnormal signal

WITH AllMonth AS (
  SELECT 
    m.Month,
    s.Vendor
  FROM 
    (SELECT DISTINCT Vendor AS Vendor FROM system_tags_aggregated) s
  CROSS JOIN (
    SELECT DISTINCT DATE_TRUNC('month', YearMonth)::DATE AS Month 
    FROM system_tags_aggregated
    WHERE YearMonth BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE
    ) m
),

MonthlyCostData AS(
  SELECT
    a.Month,
    a.Vendor,
    s.Total_Cost
  FROM AllMonth a
  LEFT JOIN (
    SELECT
    DATE_TRUNC('month', YearMonth)::DATE AS Month,
    Vendor,
    SUM(AllocationCost) AS Total_Cost
    FROM system_tags_aggregated
    WHERE YearMonth BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE
    GROUP BY ALL) s
  ON a.Vendor = s.Vendor AND a.Month = s.Month
),

ZScoreData AS (
  SELECT
    Month,
    Vendor,
    Total_Cost,
    LAG(Total_Cost) OVER (PARTITION BY Vendor ORDER BY Month) AS Lagged_Cost,
    (Total_Cost - LAG(Total_Cost) OVER (PARTITION BY Vendor ORDER BY Month)) AS Variance_Difference,
    ((Total_Cost - LAG(Total_Cost) OVER (PARTITION BY Vendor ORDER BY Month))/LAG(Total_Cost) OVER (PARTITION BY Vendor ORDER BY Month)) AS Percentage_Variance,
    AVG(Total_Cost) OVER (PARTITION BY Vendor) AS Avg_Cost,
    MEDIAN(Total_Cost) OVER (PARTITION BY Vendor) AS Median_Cost,
    STDDEV_POP(Total_Cost) OVER (PARTITION BY Vendor) AS StdDev_Cost
  FROM MonthlyCostData
)

SELECT 
  Month,
  Vendor,
  Percentage_Variance,
  Total_Cost,
  Variance_Difference,
  Avg_Cost,
  Median_Cost,
  StdDev_Cost,
  CASE
    WHEN ABS((Total_Cost - Avg_Cost) / StdDev_Cost) > 2.0
    THEN 'Anomaly'
    ELSE 'Normal'
  END AS Anomaly_Status
FROM ZScoreData
WHERE (ABS(percentage_variance) > 0.10 OR Total_Cost IS NULL) 
AND (ABS(percentage_variance) IS NOT NULL OR Lagged_Cost IS NOT NULL)  
-- to display the most recent month where either month in progress or last month contract expired resulted in null costs 
-- avoid duplicate anomalies from upcoming months
ORDER BY Vendor, Month DESC

-- COMMAND ----------

-- Anomaly Detection for Product Allocation Changes
-- Condition: 
-- 1. Allocation Percentage Changed
-- 2. Missing previous allocation


WITH AllDate AS (
  SELECT 
    a.Date,
    b.System,
    b.Component
  FROM
    (SELECT DISTINCT CAST(CONCAT(Year, '-', Month, '-01') AS DATE) AS Date FROM product_allocation) a
  CROSS JOIN
    (SELECT DISTINCT System, Component FROM product_allocation) b
),

ProductAllocation AS (
  SELECT 
  CAST(CONCAT(Year, '-', Month, '-01') AS DATE) AS Date,
  System,
  Component,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Order Cloud')) AS `Order Cloud`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Discover')) AS `Discover`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'CH One')) AS `CH One`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Content Hub')) AS `Content Hub`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'XP')) AS `XP`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Moosend')) AS `Moosend`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Connect')) AS `Connect`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Personalize')) AS `Personalize`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Send')) AS `Send`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'EXM')) AS `EXM`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'CDP')) AS `CDP`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Managed Cloud Standard')) AS `Managed Cloud Standard`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'XC')) AS `XC`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Managed Cloud Premium 2.0')) AS `Managed Cloud Premium 2.0`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Managed Cloud Premium')) AS `Managed Cloud Premium`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Search')) AS `Search`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'XM')) AS `XM`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'Connectors')) AS `Connectors`,
  (FIRST(Percentage) FILTER (WHERE `Product Name` = 'XM Cloud')) AS `XM Cloud`,
  COLLECT_LIST(`Product Name`) AS Products,
  COLLECT_LIST(Percentage) AS Percentages
FROM product_allocation
GROUP BY ALL
),

Masterlist AS(
  SELECT
    a.Date,
    a.System,
    a.Component,
    b.`Order Cloud`,
    b.`Discover`,
    b.`CH One`,
    b.`Content Hub`,
    b.`XP`,
    b.`Moosend`,
    b.`Connect`,
    b.`Personalize`,
    b.`Send`,
    b.`EXM`,
    b.`CDP`,
    b.`Managed Cloud Standard`,
    b.`XC`,
    b.`Managed Cloud Premium 2.0`,
    b.`Managed Cloud Premium`,
    b.`Search`,
    b.`XM`,
    b.`Connectors`,
    b.`XM Cloud`,
    b.Products,
    b.Percentages
  FROM AllDate a
  LEFT JOIN ProductAllocation b
  ON a.Date = b.Date AND a.System = b.System AND a.Component = b.Component
),

AnomalyDetection(
SELECT 
a.Date,
a.System,
a.Component,
a.Products AS CurrentProducts,
a.Percentages AS CurrentPercentages,
b.Products AS PrevProducts,
b.Percentages AS PrevPercentages
FROM Masterlist a
LEFT JOIN  Masterlist b
ON a.System = b.System AND a.Component = b.Component AND a.Date = ADD_MONTHS(b.Date, 1)
WHERE 
  a.`Order Cloud` <> b.`Order Cloud` OR
  a.`Discover` <> b.`Discover` OR
  a.`CH One` <> b.`CH One`  OR
  a.`Content Hub` <> b.`Content Hub` OR
  a.`XP` <> b.`XP` OR
  a.`Moosend` <> b.`Moosend` OR
  a.`Connect` <> b.`Connect` OR
  a.`Personalize` <> b.`Personalize` OR
  a.`Send` <> b.`Send` OR
  a.`EXM` <> b.`EXM` OR
  a.`CDP` <> b.`CDP` OR
  a.`Managed Cloud Standard` <> b.`Managed Cloud Standard` OR
  a.`XC` <> b.`XC` OR
  a.`Managed Cloud Premium 2.0` <> b.`Managed Cloud Premium 2.0` OR
  a.`Managed Cloud Premium` <> b.`Managed Cloud Premium` OR
  a.`Search` <> b.`Search` OR
  a.`XM` <> b.`XM` OR
  a.`Connectors` <> b.`Connectors` OR
  a.`XM Cloud` <> b.`XM Cloud` OR
  (a.`Order Cloud` IS NULL AND b.`Order Cloud` IS NOT NULL) OR
  (a.`Discover` IS NULL AND b.`Discover` IS NOT NULL) OR
  (a.`CH One` IS NULL AND b.`CH One` IS NOT NULL) OR
  (a.`Content Hub` IS NULL AND b.`Content Hub` IS NOT NULL) OR
  (a.`XP` IS NULL AND b.`XP` IS NOT NULL) OR
  (a.`Moosend` IS NULL AND b.`Moosend` IS NOT NULL) OR
  (a.`Connect` IS NULL AND b.`Connect` IS NOT NULL) OR
  (a.`Personalize` IS NULL AND b.`Personalize` IS NOT NULL) OR
  (a.`Send` IS NULL AND b.`Send` IS NOT NULL) OR
  (a.`EXM` IS NULL AND b.`EXM` IS NOT NULL) OR
  (a.`CDP` IS NULL AND b.`CDP` IS NOT NULL) OR
  (a.`Managed Cloud Standard` IS NULL AND b.`Managed Cloud Standard` IS NOT NULL) OR
  (a.`XC` IS NULL AND b.`XC` IS NOT NULL) OR
  (a.`Managed Cloud Premium 2.0` IS NULL AND b.`Managed Cloud Premium 2.0` IS NOT NULL) OR
  (a.`Managed Cloud Premium` IS NULL AND b.`Managed Cloud Premium` IS NOT NULL) OR
  (a.`Search` IS NULL AND b.`Search` IS NOT NULL) OR
  (a.`XM` IS NULL AND b.`XM` IS NOT NULL) OR
  (a.`Connectors` IS NULL AND b.`Connectors` IS NOT NULL) OR
  (a.`XM Cloud` IS NULL AND b.`XM Cloud`IS NOT NULL) 
)

Select 
*
FROM AnomalyDetection
ORDER BY Date DESC

-- COMMAND ----------

-- Z score Amomaly Detection from Product perspective

WITH AllMonth AS (
  SELECT 
    m.Month,
    s.sc_product
  FROM 
    (SELECT DISTINCT sc_product AS sc_product FROM system_tags_aggregated) s
  CROSS JOIN (
    SELECT DISTINCT DATE_TRUNC('month', YearMonth)::DATE AS Month 
    FROM system_tags_aggregated
    WHERE YearMonth BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE
    ) m
),

MonthlyCostData AS(
  SELECT
    a.Month,
    a.sc_product,
    s.Total_Cost
  FROM AllMonth a
  LEFT JOIN (
    SELECT
    DATE_TRUNC('month', YearMonth)::DATE AS Month,
    sc_product,
    SUM(AllocationCost) AS Total_Cost
    FROM system_tags_aggregated
    WHERE YearMonth BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE
    GROUP BY ALL) s
  ON a.sc_product = s.sc_product AND a.Month = s.Month
),

ZScoreData AS (
  SELECT
    Month,
    sc_product,
    Total_Cost,
    LAG(Total_Cost) OVER (PARTITION BY sc_product ORDER BY Month) AS Lagged_Cost,
    (Total_Cost - LAG(Total_Cost) OVER (PARTITION BY sc_product ORDER BY Month)) AS Variance_Difference,
    ((Total_Cost - LAG(Total_Cost) OVER (PARTITION BY sc_product ORDER BY Month))/LAG(Total_Cost) OVER (PARTITION BY sc_product ORDER BY Month)) AS Percentage_Variance,
    AVG(Total_Cost) OVER (PARTITION BY sc_product) AS Avg_Cost,
    MEDIAN(Total_Cost) OVER (PARTITION BY sc_product) AS Median_Cost,
    STDDEV_POP(Total_Cost) OVER (PARTITION BY sc_product) AS StdDev_Cost
  FROM MonthlyCostData
)

SELECT 
  Month,
  sc_product,
  Percentage_Variance,
  Total_Cost,
  Variance_Difference,
  Avg_Cost,
  Median_Cost,
  StdDev_Cost,
  CASE
    WHEN ABS((Total_Cost - Avg_Cost) / StdDev_Cost) > 2.0
    THEN 'Anomaly'
    ELSE 'Normal'
  END AS Anomaly_Status
FROM ZScoreData
WHERE ABS(percentage_variance) > 0.10 
ORDER BY sc_product, Month DESC
