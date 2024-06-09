# Databricks notebook source
# MAGIC %md
# MAGIC Batch Processing:
# MAGIC > - Description: Processes large volumes of data at scheduled intervals.
# MAGIC > - Use Case: Suitable for ETL jobs, data warehousing, and any scenario where real-time processing is not required.
# MAGIC > - Model: Processes data in chunks or batches, typically at scheduled times.
# MAGIC > - Latency: Higher latency as data is processed in large, discrete chunks.
# MAGIC
# MAGIC Streaming:
# MAGIC > - Description: Processes data in real-time as it arrives.
# MAGIC > - Use Case: Ideal for real-time analytics, monitoring, and applications requiring immediate data updates.
# MAGIC > - Model: Continuously processes data in small increments as it arrives.
# MAGIC > - Latency: Low latency, near real-time processing.
# MAGIC
# MAGIC Autoloader:
# MAGIC > - Description: Ingests data incrementally from cloud storage with automatic detection of new files.
# MAGIC > - Use Case: Useful for simplifying the ingestion of large volumes of continuously arriving data, such as logs and IoT device outputs.
# MAGIC > - Model: Ingests data incrementally by detecting new files in cloud storage.
# MAGIC > - Latency: Medium latency; designed to handle continuous data but with slight delays.
# MAGIC
# MAGIC Delta Live Table (DLT):
# MAGIC > - Description: Provides a managed service for building and managing data pipelines with built-in reliability and incremental processing.
# MAGIC > - Use Case: Best for creating declarative and reliable ETL pipelines that require minimal management effort.
# MAGIC > - Model: Declarative and incremental processing with automatic handling of dependencies and data freshness.
# MAGIC > - Latency: Can be low latency, depending on pipeline design and scheduling.

# COMMAND ----------

# DBTITLE 1,Streaming
# MAGIC %md
# MAGIC ## Streaming
# MAGIC [Structured Streaming Overview](https://www.databricks.com/spark/getting-started-with-apache-spark/streaming)
# MAGIC
# MAGIC > In Structured Streaming, a data stream is treated as a table that is being continuously appended. This leads to a stream processing model that is very similar to a batch processing model. You express your streaming computation as a standard batch-like query as on a static table, but Spark runs it as an incremental query on the unbounded input table.
# MAGIC
# MAGIC ![Streaming Unbounded table](https://www.databricks.com/sites/default/files/2020/04/gsasg-spark-streaming-workflow.png)
# MAGIC
# MAGIC A query on the input generates a result table. At every trigger interval (say, every 1 second), new rows are appended to the input table, which eventually updates the result table. Whenever the result table is updated, the changed result rows are written to an external sink. The output is defined as what gets written to external storage. The output can be configured in different modes:
# MAGIC
# MAGIC - Complete Mode: The entire updated result table is written to external storage. It is up to the storage connector to decide how to handle the writing of the entire table.
# MAGIC - Append Mode: Only new rows appended in the result table since the last trigger are written to external storage. This is applicable only for the queries where existing rows in the Result Table are not expected to change.
# MAGIC - Update Mode: Only the rows that were updated in the result table since the last trigger are written to external storage. This is different from Complete Mode in that Update Mode outputs only the rows that have changed since the last trigger. If the query doesn't contain aggregations, it is equivalent to Append mode.

# COMMAND ----------

# Initialize the stream
inputPath = "/databricks-datasets/structured-streaming/events/"

# Define the schema to speed up processing
jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

streamingInputDF = (
  spark
    .readStream
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)

# Start the streaming job
query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Autoloader
# MAGIC [What is Auto Loader?](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
# MAGIC > Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup.
# MAGIC
# MAGIC Benefits of Auto Loader over using Structured Streaming directly on files
# MAGIC In Apache Spark, you can read files incrementally using spark.readStream.format(fileFormat).load(directory). Auto Loader provides the following benefits over the file source:
# MAGIC
# MAGIC > - Scalability: Auto Loader can discover billions of files efficiently. Backfills can be performed asynchronously to avoid wasting any compute resources.
# MAGIC > - Performance: The cost of discovering files with Auto Loader scales with the number of files that are being ingested instead of the number of directories that the files may land in. See What is Auto Loader directory listing mode?.
# MAGIC > - Schema inference and evolution support: Auto Loader can detect schema drifts, notify you when schema changes happen, and rescue data that would have been otherwise ignored or lost. See How does Auto Loader schema inference work?.
# MAGIC > - Cost: Auto Loader uses native cloud APIs to get lists of files that exist in storage. In addition, Auto Loader’s file notification mode can help reduce your cloud costs further by avoiding directory listing altogether. Auto Loader can automatically set up file notification services on storage to make file discovery much cheaper.
# MAGIC
# MAGIC Schema evolution 
# MAGIC > - addNewColumns (default): Stream fails. New columns are added to the schema. Existing columns do not evolve data types.
# MAGIC > - rescue: Schema is never evolved and stream does not fail due to schema changes. All new columns are recorded in the rescued data column.
# MAGIC > - failOnNewColumns: Stream fails. Stream does not restart unless the provided schema is updated, or the offending data file is removed.
# MAGIC > - none: Does not evolve the schema, new columns are ignored, and data is not rescued unless the rescuedDataColumn option is set. Stream does not fail due to schema changes.
# MAGIC
# MAGIC Override schema inference with schema hints
# MAGIC > `.option("cloudFiles.schemaHints", "tags map<string,string>, version int")`

# COMMAND ----------

# Using Auto Loader to load to a Unity Catalog managed table

checkpoint_path = "s3://dev-bucket/_checkpoint/dev_table"

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load("s3://autoloader-source/json-data")
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable("dev_catalog.dev_database.dev_table"))

