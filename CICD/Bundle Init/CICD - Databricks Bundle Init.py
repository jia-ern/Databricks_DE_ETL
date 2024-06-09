# Databricks notebook source
# MAGIC %md
# MAGIC ### Pre-requisite: Set up Github & Databricks
# MAGIC - Databricks workspace: https://adb-5541125161551008.8.azuredatabricks.net/ 
# MAGIC - To run databricks bundle int, make sure [databricks cli version](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/#configure-your-environment-to-use-bundles ) 0.205.2 or higher is required 
# MAGIC
# MAGIC Steps:
# MAGIC 1. User CMD or Powershell, enter `databricks configure` 
# MAGIC 2. `databricks bundle init`
# MAGIC -   [Enter new bundle name]
# MAGIC -   [Enter Yes for all configurations to reain as default]
# MAGIC 3. Modify or insert the yml file under resource folder to be deployed
# MAGIC 4. To deploy latest bundle to development environment: `databricks bundle deploy --target dev `
# MAGIC 5. To deploy latest bundle to production environment: `databricks bundle deploy --target prod `
