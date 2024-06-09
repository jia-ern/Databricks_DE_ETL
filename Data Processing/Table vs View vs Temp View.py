# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Table
# MAGIC - A table is a database entity that stores data in the form of rows and columns.
# MAGIC - A table stores the data.
# MAGIC - A table can only be created or dropped.
# MAGIC - A table is an independent database entity.
# MAGIC - It is stored in physical storage as it occupies real space on systems.
# MAGIC - A table gives results faster.
# MAGIC - We can add, delete or update the data in a table.
# MAGIC ### View
# MAGIC - A view only extracts data from the table.
# MAGIC - A view is a virtual table used to view or manipulate some parts of the table. It also has rows and columns as real ordered tables.
# MAGIC - A view can be recreated.
# MAGIC - A view is dependent on the table.
# MAGIC - It is not stored physically. It only requires some space in memory whenever we run its query.
# MAGIC - A view gives results slower because it has to run its queries each time to retrieve the information from the table.
# MAGIC - We cannot modify data from a view. We can change the data in the base table.
# MAGIC
# MAGIC ### Temp 
# MAGIC - Temp tables which are created to temporarily store data.
# MAGIC - Temp tables gives you the ability to allow all users access to a table while limiting what they can see and protecting sensitive information. 
# MAGIC - Temp tables have the ability to be indexed.
# MAGIC
# MAGIC
# MAGIC ### CTE
# MAGIC - can think of a Common Table Expression (CTE) as a table subquery. 
# MAGIC - AKA. derived table, is a query that is used as the starting point to build another query. 
# MAGIC - exist only for the duration of the query. 
# MAGIC - make the code easier to write as you can write the CTEs at the top of your query.
# MAGIC - can have more than one CTE, and CTEs can reference other CTEs – and then you can use the defined CTEs in your main query.

# COMMAND ----------

database = "default"

# Table
vendor_data_table = f"{database}.vendor_data_table"
azure_data = f"{database}.azure_data"
cdp_data = f"{database}.cdp_data"
vendor_cost_override = f"{database}.vendor_cost_override"

# View
vendor_data_view = "vendor_data_view"
vendors_cost_override_view = "vendors_cost_override_view"

# COMMAND ----------

# DBTITLE 1,Create Table
spark.sql(f"""
CREATE OR REPLACE TABLE {database}.{vendor_data_table} AS
(
    ( -- Azure
        SELECT 
        --System
        SubscriptionId, SubscriptionName, SubAccount, ResourceGroup, ResourceLocation, Date, PricingModel, InvoiceSectionName, CostType, System, Product, Vendor, VendorService, Publisher,
        --Tags
        sc_system, sc_component, sc_env, sc_region, sc_type, sc_costowner, sc_createdby, sc_provider,
        --Misc
        Year, Month, Cost AS AllocationCost
        FROM {database}.{azure_data}
    )
    UNION ALL
    ( -- CDP
        SELECT 
        --System
        NULL AS SubscriptionId, NULL AS SubscriptionName, SubAccount, ResourceGroup, ResourceLocation, Date, PricingModel, NULL AS InvoiceSectionName, 
        NULL AS CostType, System, NULL AS Product, Vendor, VendorService, NULL AS Publisher,
        --Tags
        sc_system, sc_component, sc_env, sc_region, sc_type, sc_costowner, sc_createdby, sc_provider,
        --Misc
        Year, Month, AllocationCost
        FROM {database}.{cdp_data}
    )
    UNION ALL
    ( -- Vendor Cost Override
        SELECT * FROM {database}.{vendor_cost_override}
    )
)
""")

# COMMAND ----------

# DBTITLE 1,Create View
spark.sql(f"""
CREATE OR REPLACE VIEW {database}.{vendor_data_view} AS
(
    ( -- Azure
        SELECT 
        --System
        SubscriptionId, SubscriptionName, SubAccount, ResourceGroup, ResourceLocation, Date, PricingModel, InvoiceSectionName, CostType, System, Product, Vendor, VendorService, Publisher,
        --Tags
        sc_system, sc_component, sc_env, sc_region, sc_type, sc_costowner, sc_createdby, sc_provider,
        --Misc
        Year, Month, Cost AS AllocationCost
        FROM {database}.{azure_data}
    )
    UNION ALL
    ( -- CDP
        SELECT 
        --System
        NULL AS SubscriptionId, NULL AS SubscriptionName, SubAccount, ResourceGroup, ResourceLocation, Date, PricingModel, NULL AS InvoiceSectionName, 
        NULL AS CostType, System, NULL AS Product, Vendor, VendorService, NULL AS Publisher,
        --Tags
        sc_system, sc_component, sc_env, sc_region, sc_type, sc_costowner, sc_createdby, sc_provider,
        --Misc
        Year, Month, AllocationCost
        FROM {database}.{cdp_data}
    )
    UNION ALL
    ( -- Vendor Cost Override
        SELECT * FROM {database}.{vendor_cost_override}
    )
)
""")

# COMMAND ----------

# DBTITLE 1,Create cte
df = spark.sql(f"""
    WITH cte (
        ( -- Azure
            SELECT 
            --System
            SubscriptionId, SubscriptionName, SubAccount, ResourceGroup, ResourceLocation, Date, PricingModel, InvoiceSectionName, CostType, System, Product, Vendor, VendorService, Publisher,
            --Tags
            sc_system, sc_component, sc_env, sc_region, sc_type, sc_costowner, sc_createdby, sc_provider,
            --Misc
            Year, Month, Cost AS AllocationCost
            FROM {database}.{azure_data}
        )
        UNION ALL
        ( -- CDP
            SELECT 
            --System
            NULL AS SubscriptionId, NULL AS SubscriptionName, SubAccount, ResourceGroup, ResourceLocation, Date, PricingModel, NULL AS InvoiceSectionName, 
            NULL AS CostType, System, NULL AS Product, Vendor, VendorService, NULL AS Publisher,
            --Tags
            sc_system, sc_component, sc_env, sc_region, sc_type, sc_costowner, sc_createdby, sc_provider,
            --Misc
            Year, Month, AllocationCost
            FROM {database}.{cdp_data}
        )
        UNION ALL
        ( -- Vendor Cost Override
            SELECT * FROM {database}.{vendor_cost_override}
        )
    )
    SELECT * FROM cte
""")
df.createOrReplaceTempView(vendors_cost_override_view)
