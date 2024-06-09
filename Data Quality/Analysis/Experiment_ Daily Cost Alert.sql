-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Product Cost Anomaly**

-- COMMAND ----------


CREATE OR REPLACE TEMPORARY VIEW ProductAnomalyStatus AS(

-- Get all available date
WITH AllMonth AS (
  SELECT 
    Month
  FROM 
    (SELECT DISTINCT YearMonth AS Month FROM system_tags_aggregated) 
),

-- Get all month with all product cost
MonthlyCostData AS(
  SELECT
    a.Month,
    b.sc_product,
    b.Total_Cost
  FROM AllMonth a
  LEFT JOIN (
    SELECT
    DATE_TRUNC('month', YearMonth)::DATE AS Month,
    sc_product,
    SUM(AllocationCost) AS Total_Cost
    FROM system_tags_aggregated
    WHERE YearMonth BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE
    GROUP BY ALL) b
  ON a.Month = b.Month 
),

-- Compare the product cost between current month and previous month
-- Show the percentage variance and cost different
CostVariance AS (
  SELECT
    Month,
    CASE
      WHEN sc_product IS NULL THEN 'Not Allocated to Product'
      ELSE sc_product
    END AS sc_product,
    Total_Cost,
    LAG(Total_Cost) OVER (PARTITION BY sc_product ORDER BY Month) AS Lagged_Cost,
    (Total_Cost - LAG(Total_Cost) OVER (PARTITION BY sc_product ORDER BY Month)) AS Variance_Difference,
    ((Total_Cost - LAG(Total_Cost) OVER (PARTITION BY sc_product ORDER BY Month))/LAG(Total_Cost) OVER (PARTITION BY sc_product ORDER BY Month)) AS Percentage_Variance
  FROM MonthlyCostData
)

-- Detect the product cost variance more than +- 10% 
SELECT 
  Month AS Date,
  Year(Month) AS Year,
  Month(Month) AS Month,
  sc_product,
  Percentage_Variance,
  Total_Cost,
  Lagged_Cost,
  Variance_Difference,
  CASE
    WHEN Percentage_Variance > 0.1 THEN 'Anomaly'
    WHEN Percentage_Variance < -0.1 THEN 'Anomaly'
    ELSE 'Normal'
  END AS Anomaly_Status
FROM CostVariance
)

-- COMMAND ----------

SELECT * FROM ProductAnomalyStatus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Vendor Data Onboarding Check & Variance changes**

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW VendorbyProduct_AnomalyStatus AS(

-- Get all available date
WITH AllMonth AS (
  SELECT
    a.Month,
    b.Vendor,
    CASE
      WHEN b.sc_product IS NULL THEN 'Not Allocated to Product'
      ELSE b.sc_product
    END AS sc_product
  FROM 
    (SELECT DISTINCT YearMonth AS Month FROM system_tags_aggregated
    WHERE YearMonth BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE) a
    CROSS JOIN
    (SELECT DISTINCT Vendor, sc_product FROM system_tags_aggregated
    WHERE YearMonth BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE) b
    
),

-- Get all month with all product cost
MonthlyCostData AS(
  SELECT
    a.Month,
    a.sc_product,
    a.Vendor,
    b.Total_Cost
  FROM AllMonth a
  LEFT JOIN (
    SELECT
    YearMonth AS Month,
    CASE
      WHEN sc_product IS NULL THEN 'Not Allocated to Product'
      ELSE sc_product
    END AS sc_product,
    Vendor,
    SUM(AllocationCost) AS Total_Cost
    FROM system_tags_aggregated
    WHERE YearMonth BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE
    GROUP BY ALL) b
  ON a.Month = b.Month AND a.sc_product = b.sc_product AND a.Vendor = b.Vendor
),

-- Compare the vendor cost between current month and previous month
-- Show the percentage variance and cost different
VendorCostVariance AS (
  SELECT
    Month,
    sc_product,
    Vendor,
    Total_Cost,
    LAG(Total_Cost) OVER (PARTITION BY sc_product, Vendor ORDER BY Month) AS Lagged_Cost,
    (Total_Cost - LAG(Total_Cost) OVER (PARTITION BY sc_product, Vendor ORDER BY Month)) AS Variance_Difference,
    ((Total_Cost - LAG(Total_Cost) OVER (PARTITION BY sc_product, Vendor ORDER BY Month))/LAG(Total_Cost) OVER (PARTITION BY sc_product, Vendor ORDER BY Month)) AS Percentage_Variance
  FROM MonthlyCostData
)

-- Detect the vendor by product cost variance more than +- 10% 
-- Detect the vendor data is not available yet
SELECT
Month AS Date,
Year(Month) AS Year,
Month(Month) AS Month,
  sc_product,
  Vendor,
  CASE
    WHEN Total_Cost IS NULL THEN 0
    ELSE Total_Cost
  END AS Total_Cost,
  Lagged_Cost,
  Variance_Difference,
  Percentage_Variance,
  CASE
    WHEN Total_Cost IS NULL THEN 'Data Not Avaible Yet'
    WHEN Percentage_Variance > 0.1 THEN 'Anomaly'
    WHEN Percentage_Variance < -0.1 THEN 'Anomaly'
    ELSE 'Normal'
  END AS VendorAnomalyStatus
FROM VendorCostVariance 
)



-- COMMAND ----------

SELECT * FROM VendorbyProduct_AnomalyStatus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC **Azure Daily Moving Average**

-- COMMAND ----------


-- Join daily vendor data view with Product Allocation
CREATE OR REPLACE TEMPORARY VIEW AzureDailyMovingAverage AS(

WITH DailyAzureData AS (
SELECT a.*, 
pa.`Product Name` AS sc_product,
(a.AllocationCost * COALESCE(pa.Percentage, 1)) AS ProductAllocationCost
FROM hive_metastore.default.vendor_data_view a
LEFT JOIN hive_metastore.default.product_allocation pa
ON a.sc_system = pa.`System` AND pa.Component = COALESCE(a.sc_component, '')  AND a.Year = pa.Year AND a.Month = pa.`Month`
WHERE Vendor = 'Azure' AND Date BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE
),

-- Aggregate cost by sc_product
AggregatedProductCost AS(
SELECT
  Date,
  Vendor,
  CASE
    WHEN sc_product IS NULL THEN 'Not Allocated to Product'
    ELSE sc_product
  END AS sc_product,
  Sum(ProductAllocationCost) AS Cost
FROM DailyAzureData
GROUP BY ALL
)

SELECT 
  Date,
  Vendor,
  sc_product,
  Cost,
  AVG(Cost) OVER (PARTITION BY Vendor, sc_product ORDER BY DATE ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS MovingAverage
FROM AggregatedProductCost
)

-- COMMAND ----------

SELECT * FROM AzureDailyMovingAverage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC **Product Allocation Changes**

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW AllocationStatus AS(

-- Get all available date
WITH AllMonth AS (
  SELECT
    b.Year,
    b.Month,
    a.System,
    a.Component,
    a.`Product Name` AS sc_product
  FROM
    (SELECT DISTINCT System, Component, `Product Name` FROM product_allocation) a
  CROSS JOIN
    (SELECT DISTINCT Year, Month FROM product_allocation) b
),

-- Get all month with all product allocation
ProductAllocationAllMonth AS(
SELECT 
  a.Year,
  a.Month,
  a.System,
  a.Component,
  a.sc_product,
  b.Percentage
FROM AllMonth a
LEFT JOIN 
  product_allocation b ON a.Year = b.Year AND a.Month = b.Month AND a.System = b.System AND a.Component = b.Component AND a.sc_product = b.`Product Name`
)

-- Compare the allocation percentage between current month and previous month
-- Show the allocation status
SELECT 
  CAST(CONCAT(Year, '-', LPAD(Month, 2, '0'), '-01') AS Date) AS Date, 
  Year,
  Month,
  System,
  Component,
  sc_product,
    (LAG(Percentage) OVER (PARTITION BY System, Component, sc_product ORDER BY Year, Month)) AS PrevMonth_Percentage,
    Percentage AS CurrentMonth_Percentage,
    CASE
      WHEN Percentage IS NOT NULL AND PrevMonth_Percentage = Percentage THEN 'Percentage Remain Unchanged' 
      WHEN Percentage IS NOT NULL AND Percentage > PrevMonth_Percentage THEN 'Percentage Increase'
      WHEN Percentage IS NOT NULL AND Percentage < PrevMonth_Percentage THEN 'Percentage Decrease'
      WHEN Percentage IS NOT NULL AND PrevMonth_Percentage IS NULL THEN 'Input Product Allocation'
      WHEN Percentage IS NULL AND PrevMonth_Percentage IS NULL THEN 'NULL'
      WHEN Percentage IS NULL AND PrevMonth_Percentage IS NOT NULL THEN 'Missing Allocation'
      ELSE 'NEED TO CHECK FURTHER'
    END AS `Allocation Status`
  FROM ProductAllocationAllMonth
)

-- COMMAND ----------

SELECT * FROM AllocationStatus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Data Checking**

-- COMMAND ----------

SELECT 
  a.system_tags_aggregated, 
  b.ProductAnomalyStatus,
  c.VendorbyProduct_AnomalyStatus
FROM (SELECT SUM(AllocationCost) AS system_tags_aggregated
FROM system_tags_aggregated
WHERE YearMonth BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE) a

CROSS JOIN
(SELECT SUM(Total_Cost) AS ProductAnomalyStatus
FROM ProductAnomalyStatus) b

CROSS JOIN
(SELECT SUM(Total_Cost) AS VendorbyProduct_AnomalyStatus
FROM VendorbyProduct_AnomalyStatus) c

-- COMMAND ----------

SELECT a.vendor_data_view_azure, b.AzureDailyMovingAverage

FROM (SELECT SUM(AllocationCost) AS vendor_data_view_azure FROM vendor_data_view
WHERE Vendor = 'Azure' AND Date BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND CURRENT_DATE) a

CROSS JOIN

(SELECT SUM(Cost) AS AzureDailyMovingAverage from AzureDailyMovingAverage) b

-- COMMAND ----------

SELECT 
  Year,
  Month,
  System,
  Component,
  ROUND(Sum(PrevMonth_Percentage),0) AS PrevMonth_TotalPercentage,
  ROUND(Sum(CurrentMonth_Percentage),0) AS CurrentMonth_TotalPercentage
FROM AllocationStatus
GROUP BY ALL
HAVING PrevMonth_TotalPercentage NOT IN (NULL, 1) OR CurrentMonth_TotalPercentage NOT IN (NULL, 1)

