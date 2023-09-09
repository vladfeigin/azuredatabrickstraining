# Databricks notebook source
# MAGIC %md
# MAGIC ## Python UDF
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.widgets.removeAll()
# MAGIC dbutils.widgets.text("storage_account", "")
# MAGIC dbutils.widgets.text("container_name", "")
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC storage_account = getArgument("storage_account")
# MAGIC container_name = getArgument("container_name")

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
# MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwlmeo&st=2023-09-07T14:17:14Z&se=2023-11-30T23:17:14Z&spr=https&sv=2022-11-02&sr=c&sig=jyWEvg%2FzLmK9J%2BOxIp%2B8QSCKYpVmNPfKNcNIo68Rh6E%3D")

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flight_delay_bronze

# COMMAND ----------

flight_delay_bronze_df = _sqldf

# COMMAND ----------

# MAGIC
# MAGIC %md ### User-Defined Function (UDF)
# MAGIC A custom column transformation function
# MAGIC
# MAGIC - Can’t be optimized by Catalyst Optimizer, so it's slower comaring to regular Spark functions
# MAGIC - Function is serialized and sent to executors
# MAGIC - Row data is deserialized from Spark's native binary format to pass to the UDF, and the results are serialized back into Spark's native format
# MAGIC - For Python UDFs, additional interprocess communication overhead between the executor and a Python interpreter running on each worker node
# MAGIC

# COMMAND ----------

import math
def time_to_bin(time:int ):
    return math.floor(time / 100)

time_to_bin(456)

# COMMAND ----------

time_to_bin_udf = udf(time_to_bin)

# COMMAND ----------

from pyspark.sql.functions import col

display(flight_delay_bronze_df.select(time_to_bin_udf(col("CRSDepTime"))))

# COMMAND ----------

# MAGIC %md ### Register UDF to use in SQL
# MAGIC Register the UDF using **`spark.udf.register`** to also make it available for use in the SQL.
# MAGIC

# COMMAND ----------

time_to_bin_udf = spark.udf.register("time_to_bin_udf", time_to_bin)

# COMMAND ----------

flight_delay_bronze_df.createOrReplaceTempView("flight_delay_bronze_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select time_to_bin_udf(CRSDepTime) from flight_delay_bronze_view

# COMMAND ----------

# MAGIC %md ### Pandas/Vectorized UDFs
# MAGIC
# MAGIC Pandas UDFs are available in Python to improve the efficiency of UDFs. Pandas UDFs utilize Apache Arrow to speed up computation.
# MAGIC
# MAGIC * <a href="https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html" target="_blank">Blog post</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow" target="_blank">Documentation</a>
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" alt="Benchmark" width ="500" height="1500">
# MAGIC
# MAGIC The user-defined functions are executed using: 
# MAGIC * <a href="https://arrow.apache.org/" target="_blank">Apache Arrow</a>, an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes with near-zero (de)serialization cost
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

import math
import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("int")
def vectorized_time_to_bin_udf(time: pd.Series) -> pd.Series:
     return (time / 100).apply(math.floor)

# COMMAND ----------

from pyspark.sql.functions import col

display(flight_delay_bronze_df.select(vectorized_time_to_bin_udf(col("CRSDepTime"))))

# COMMAND ----------

# MAGIC %md
# MAGIC Pandas SQL works also with SQL

# COMMAND ----------

spark.udf.register("vectorized_time_to_bin_udf", vectorized_time_to_bin_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select vectorized_time_to_bin_udf(CRSDepTime) from flight_delay_bronze_view

# COMMAND ----------


