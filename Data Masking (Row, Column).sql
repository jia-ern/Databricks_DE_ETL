-- Databricks notebook source
USE CATALOG costreporting_dev

-- COMMAND ----------

-- DBTITLE 1,Clone 2024 records
CREATE OR REPLACE TABLE bronze.jif_table AS (
  SELECT * FROM bronze.src_rackspace
  WHERE Year(InvoiceDate) = 2024
)

-- COMMAND ----------

SELECT * FROM bronze.jif_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Filter to export to spcific Customer based on CustomerNumber

-- COMMAND ----------

-- DBTITLE 1,Create Function
CREATE OR REPLACE FUNCTION customerNo_filter(CustomerNumber STRING)
RETURN CustomerNumber='020-1294357';

-- COMMAND ----------

-- DBTITLE 1,Create table and apply function using Alter
CREATE OR REPLACE TABLE bronze.jif_table_filtered AS (
  SELECT * FROM bronze.jif_table
);
ALTER TABLE bronze.jif_table_filtered SET ROW FILTER customerNo_filter ON (CustomerNumber);

-- COMMAND ----------

SELECT * FROM bronze.jif_table_filtered

-- COMMAND ----------

-- DBTITLE 1,DROP ROW FILTER
ALTER TABLE bronze.jif_table_filtered DROP ROW FILTER;

-- COMMAND ----------

SELECT * FROM bronze.jif_table_filtered

-- COMMAND ----------

-- DBTITLE 1,Create a table with the function applied as a row filter
CREATE OR REPLACE TABLE bronze.jif_table_filtered (
  Vendor STRING,
  InvoiceNumber STRING,
  InvoiceDate DATE,
  CustomerNumber STRING
)
WITH ROW FILTER customerNo_filter ON (CustomerNumber);

CREATE OR REPLACE TABLE bronze.jif_table_filtered AS (
  SELECT Vendor, InvoiceNumber, InvoiceDate, CustomerNumber 
  FROM bronze.jif_table
)

-- COMMAND ----------

SELECT * FROM bronze.jif_table_filtered

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Filter to mask InvoiceNumber

-- COMMAND ----------

CREATE OR REPLACE FUNCTION invoiceNo_mask(InvoiceNumber STRING)
  RETURN '**-********';
  --RETURN CASE WHEN is_member('HumanResourceDept') THEN ssn ELSE '***-**-****' END;

-- COMMAND ----------

ALTER TABLE bronze.jif_table_filtered ALTER COLUMN InvoiceNumber SET MASK invoiceNo_mask;

-- COMMAND ----------

SELECT * FROM bronze.jif_table_filtered

-- COMMAND ----------

-- DBTITLE 1,disable the column mask
ALTER TABLE bronze.jif_table_filtered ALTER COLUMN InvoiceNumber DROP MASK;

-- COMMAND ----------

SELECT * FROM bronze.jif_table_filtered

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use mapping tables to create an access-control list
-- MAGIC
-- MAGIC - Allow specific user to read the table only

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.jif_valid_InvoiceNumber(username string);
INSERT INTO bronze.jif_valid_InvoiceNumber
VALUES
  ('jiaern.goo@sitecore.com');

-- COMMAND ----------

SELECT * FROM bronze.jif_valid_InvoiceNumber

-- COMMAND ----------

CREATE OR REPLACE FUNCTION row_filter()
  RETURN EXISTS(
    SELECT 1 FROM bronze.jif_valid_InvoiceNumber v
    WHERE v.username = CURRENT_USER()
);

-- COMMAND ----------

DROP TABLE bronze.jif_table_filtered;

CREATE OR REPLACE TABLE bronze.jif_table_filtered (
  Vendor STRING,
  InvoiceNumber STRING,
  InvoiceDate DATE,
  CustomerNumber STRING
)
WITH ROW FILTER row_filter ON ();

CREATE OR REPLACE TABLE bronze.jif_table_filtered AS (
  SELECT Vendor, InvoiceNumber, InvoiceDate, CustomerNumber 
  FROM bronze.jif_table
)

-- COMMAND ----------

SELECT * FROM bronze.jif_table_filtered

-- COMMAND ----------

DROP TABLE bronze.jif_valid_InvoiceNumber;
DROP TABLE bronze.jif_table_filtered;
DROP TABLE bronze.jif_table;
