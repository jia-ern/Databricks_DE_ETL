# Databricks notebook source
dbutils.widgets.text(name="catalog",defaultValue="",label=" Enter the catalog in lower case")
catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text(name="env",defaultValue="",label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

def create_Bronze_Schema(container):
    print(f'Using hive_metastore')
    spark.sql(f""" USE CATALOG hive_metastore""")
    print(f'Creating Bronze Schema in hive_metastore')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `bronze`""")
    print("************************************")

# COMMAND ----------

def create_Silver_Schema(container):
    print(f'Using hive_metastore')
    spark.sql(f""" USE CATALOG 'hive_metastore'""")
    print(f'Creating Silver Schema in hive_metastore')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `silver`""")
    print("************************************")

# COMMAND ----------

def create_Gold_Schema(container):
    print(f'Using hive_metastore')
    spark.sql(f""" USE CATALOG 'hive_metastore'""")
    print(f'Creating Gold Schema in hive_metastore')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `gold`""")
    #spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `gold` MANAGED LOCATION '{path}/{container}'""")
    print("************************************")

# COMMAND ----------

create_Bronze_Schema("bronze")

# COMMAND ----------

create_Silver_Schema("silver")

# COMMAND ----------

create_Gold_Schema("gold")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating Bronze Tables

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating raw_traffic table

# COMMAND ----------

def createTable_rawTraffic(catalog, env):
    print(f'Creating raw_Traffic table in {catalog}')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{catalog}`.`bronze`.`raw_traffic_{env}`
                        (
                            Record_ID INT,
                            Count_point_id INT,
                            Direction_of_travel VARCHAR(255),
                            Year INT,
                            Count_date VARCHAR(255),
                            hour INT,
                            Region_id INT,
                            Region_name VARCHAR(255),
                            Local_authority_name VARCHAR(255),
                            Road_name VARCHAR(255),
                            Road_Category_ID INT,
                            Start_junction_road_name VARCHAR(255),
                            End_junction_road_name VARCHAR(255),
                            Latitude DOUBLE,
                            Longitude DOUBLE,
                            Link_length_km DOUBLE,
                            Pedal_cycles INT,
                            Two_wheeled_motor_vehicles INT,
                            Cars_and_taxis INT,
                            Buses_and_coaches INT,
                            LGV_Type INT,
                            HGV_Type INT,
                            EV_Car INT,
                            EV_Bike INT,
                            Extract_Time TIMESTAMP
                    );""")
    
    print("************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating raw_roads Table

# COMMAND ----------

def createTable_rawRoad(catalog, env):
    print(f'Creating raw_roads table in {catalog}')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{catalog}`.`bronze`.`raw_roads_{env}`
                        (
                            Road_ID INT,
                            Road_Category_Id INT,
                            Road_Category VARCHAR(255),
                            Region_ID INT,
                            Region_Name VARCHAR(255),
                            Total_Link_Length_Km DOUBLE,
                            Total_Link_Length_Miles DOUBLE,
                            All_Motor_Vehicles DOUBLE
                    );""")
    
    print("************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calling all functions

# COMMAND ----------

createTable_rawTraffic("hive_metastore")
createTable_rawRoad("hive_metastore")
