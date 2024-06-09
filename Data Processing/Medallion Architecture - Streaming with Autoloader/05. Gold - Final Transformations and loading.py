# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Calling common notebook to re-use variables

# COMMAND ----------

# MAGIC %run "/Users/jif@sitecore.net/Autoloader Streaming + Unity Catalog/notebooks/00. Common"

# COMMAND ----------

dbutils.widgets.text(name="catalog",defaultValue="",label=" Enter the catalog in lower case")
catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.storagejif01.dfs.core.windows.net",
    "Q4XwAdeVnRBYFhf2AbumNbtePQ2jWu9ddsYYC4Wj3FZKP0Q1bH+4d+bCcuiN3FhRr5uDbDoTh3pV+AStYzfCNQ==")

# COMMAND ----------

checkpoint = "abfss://checkpoints@storagejif01.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read Silver_Traffic table

# COMMAND ----------


def read_SilverTrafficTable(catalog, environment):
    print('Reading the Silver Traffic Table Data : ',end='')
    df_SilverTraffic = (spark.readStream
                    .table(f"`{catalog}`.`silver`.silver_traffic_{environment}")
                    )
    print(f'Reading {catalog}.silver.silver_traffic_{environment} Success!')
    print("**********************************")
    return df_SilverTraffic

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read silver_roads Table

# COMMAND ----------


def read_SilverRoadsTable(catalog, environment):
    print('Reading the Silver Table Silver_roads Data : ',end='')
    df_SilverRoads = (spark.readStream
                    .table(f"`{catalog}`.`silver`.silver_roads_{environment}")
                    )
    print(f'Reading {catalog}.silver.silver_roads_{environment} Success!')
    print("**********************************")
    return df_SilverRoads

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating vehicle Intensity Column

# COMMAND ----------

def create_VehicleIntensity(df):
 from pyspark.sql.functions import col
 print('Creating Vehicle Intensity column : ',end='')
 df_veh = df.withColumn('Vehicle_Intensity',
               col('Motor_Vehicles_Count') / col('Link_length_km')
               )
 print("Success!!!")
 print('***************')
 return df_veh

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating LoadTime column

# COMMAND ----------

def create_LoadTime(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Load Time column : ',end='')
    df_timestamp = df.withColumn('Load_Time',
                      current_timestamp()
                      )
    print('Success!!')
    print('**************')
    return df_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Writing Data to Gold Traffic
# MAGIC

# COMMAND ----------

def write_Traffic_GoldTable(StreamingDF,catalog,environment):
    print('Writing the gold_traffic Data : ',end='') 

    write_gold_traffic = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/GoldTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{catalog}`.`gold`.`gold_traffic_{environment}`"))
    
    write_gold_traffic.awaitTermination()
    print(f'Writing `{catalog}`.`gold`.`gold_traffic_{environment}` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Writing Data to Gold Roads
# MAGIC

# COMMAND ----------

def write_Roads_GoldTable(StreamingDF,catalog, environment):
    print('Writing the gold_roads Data : ',end='') 

    write_gold_roads = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/GoldRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{catalog}`.`gold`.`gold_roads_{environment}`"))
    
    write_gold_roads.awaitTermination()
    print(f'Writing `{catalog}`.`gold`.`gold_roads_{environment}` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calling all functions
# MAGIC

# COMMAND ----------

## Reading from Silver tables
df_SilverTraffic = read_SilverTrafficTable(catalog,env)
df_SilverRoads = read_SilverRoadsTable(catalog,env)
    
## Tranformations     
df_vehicle = create_VehicleIntensity(df_SilverTraffic)
df_FinalTraffic = create_LoadTime(df_vehicle)
df_FinalRoads = create_LoadTime(df_SilverRoads)


## Writing to gold tables    
write_Traffic_GoldTable(df_FinalTraffic,catalog,env)
write_Roads_GoldTable(df_FinalRoads,catalog,env)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM `hive_metastore`.`gold`.`gold_roads_dev`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM `hive_metastore`.`gold`.`gold_traffic_dev`
