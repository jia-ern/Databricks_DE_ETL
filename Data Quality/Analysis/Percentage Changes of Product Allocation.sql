-- Databricks notebook source
-- TO HAVE ALL MONTHS IN THE TABLE --
CREATE OR REPLACE TEMPORARY VIEW SystemComponentAllMonth
AS
  SELECT
    2023 AS Year,
    m.Month AS Month,
    s.System,
    s.Component,
    s.`Product Name`
  FROM
    (SELECT DISTINCT system, Component, `Product Name` FROM product_allocation) s
  CROSS JOIN
    (SELECT DISTINCT MONTH FROM product_allocation WHERE Year = 2023 AND MONTH BETWEEN 6 AND 12) m
  ORDER BY System, s.Component, Year, Month

-- COMMAND ----------

SELECT * FROM SystemComponentAllMonth

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ProductAllocationAllMonth
AS
  SELECT 
    a.Year,
    a.Month,
    a.System,
    a.Component,
    a.`Product Name`,
    b.Percentage
  FROM SystemComponentAllMonth a
  LEFT JOIN 
    product_allocation b ON a.Year = b.Year AND a.Month = b.Month AND a.System = b.System AND a.Component = b.Component AND a.`Product Name` = b.`Product Name`


-- COMMAND ----------

SELECT * FROM ProductAllocationAllMonth

-- COMMAND ----------

-- ALLOCATION PERCENTAGE CHANGES STATUS --
CREATE OR REPLACE TEMPORARY VIEW MonthlyChanges
AS
  SELECT 
    Year,
    Month,
    System,
    Component,
    `Product Name`,
    (LAG(Percentage) OVER (PARTITION BY System, Component, `Product Name` ORDER BY Year, Month)) AS PrevMonth_Percentage,
    Percentage AS CurrentMonth_Percentage,
    LEAD(Percentage,1) OVER (PARTITION BY System, Component,  `Product Name` ORDER BY Year, Month) AS NextMonth_Percentage,
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
  WHERE YEAR = 2023 AND MONTH >= 7

-- COMMAND ----------

SELECT * FROM MonthlyChanges

-- COMMAND ----------

SELECT
  b.Year,
  b.Month,
  a.System,
  a.Component,
  b.`Product Name`,
  b.`CurrentMonth_Percentage` 
  FROM 
    (SELECT 
      System,
      Component
    FROM MonthlyChanges 
    WHERE `Allocation Status` IN ('Percentage Increase' ,'Percentage Decrease')
    GROUP BY Year, Month, System, Component
    ORDER BY System, Component, Year, Month) a
LEFT JOIN MonthlyChanges b
ON a.System = b.System AND a.Component = b.Component
ORDER BY System, Component, `Product Name`, Year, Month

