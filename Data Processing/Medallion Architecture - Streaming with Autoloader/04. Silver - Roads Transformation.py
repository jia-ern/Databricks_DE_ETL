# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Running common notebook to get access to variables

# COMMAND ----------

# MAGIC %run "/Users/jif@sitecore.net/Autoloader Streaming + Unity Catalog/notebooks/00. Common"

# COMMAND ----------

dbutils.widgets.text(name="catalog",defaultValue="",label=" Enter the catalog in lower case")
catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Reading from bronze raw_Roads

# COMMAND ----------


def read_BronzeRoadsTable(catalog, environment):
    print('Reading the Bronze Table raw_roads Data : ',end='')
    df_bronzeRoads = (spark.readStream
                    .table(f"`{catalog}`.`bronze`.raw_roads_{environment}")
                    )
    print(f'Reading {catalog}.bronze.raw_roads_{environment} Success!')
    print("**********************************")
    return df_bronzeRoads

# COMMAND ----------

df_roads = read_BronzeRoadsTable(catalog,env)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating road_category_name column

# COMMAND ----------

def road_Category(df):
    print('Creating Road Category Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_Cat = df.withColumn("Road_Category_Name",
                  when(col('Road_Category') == 'TA', 'Class A Trunk Road')
                  .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
                   .when(col('Road_Category') == 'PA','Class A Principal road')
                    .when(col('Road_Category') == 'PM','Class A Principal Motorway')
                    .when(col('Road_Category') == 'M','Class B road')
                    .otherwise('NA')
                  
                  )
    print('Success!! ')
    print('***********************')
    return df_road_Cat

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating road_type column
# MAGIC
# MAGIC

# COMMAND ----------

def road_Type(df):
    print('Creating Road Type Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_Type = df.withColumn("Road_Type",
                  when(col('Road_Category_Name').like('%Class A%'),'Major')
                  .when(col('Road_Category_Name').like('%Class B%'),'Minor')
                    .otherwise('NA')
                  
                  )
    print('Success!! ')
    print('***********************')
    return df_road_Type

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Writing data to silver_roads in Silver schema

# COMMAND ----------

def write_Roads_SilverTable(StreamingDF,catalog,environment):
    print('Writing the silver_roads Data : ',end='') 

    write_StreamSilver_R = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/SilverRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{catalog}`.`silver`.`silver_roads_{environment}`"))
    
    write_StreamSilver_R.awaitTermination()
    print(f'Writing `{catalog}`.`silver`.`silver_roads_{environment}` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Calling functions

# COMMAND ----------

df_noDups = remove_Dups(df_roads)

AllColumns = df_noDups.schema.names
df_clean = handle_NULLs(df_noDups,AllColumns)

## Creating Road_Category_name 
df_roadCat = road_Category(df_clean)

## Creating Road_Type column
df_type = road_Type(df_roadCat)

## Writing data to silver_roads table

write_Roads_SilverTable(df_type,catalog,env)

# COMMAND ----------

spark.sql(f"""
          SELECT COUNT(*) FROM `hive_metastore`.`silver`.`silver_roads_{env}`
          """)

# COMMAND ----------

spark.sql(f"""
          SELECT * FROM `hive_metastore`.`silver`.`silver_roads_{env}`
          """)
