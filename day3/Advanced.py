# Databricks notebook source


# COMMAND ----------

# MAGIC %sql
# MAGIC use flights

# COMMAND ----------

# MAGIC %sql select * from flight_delay_bronze

# COMMAND ----------

df = spark.readStream \
.option("ignoreChanges", "true") \
.option("maxBytesPerTrigger", 1024) \
.format("delta") \
.table() \
.select("*") \
.select(F.to_json(F.struct("*")).alias("body")).drop('*')
