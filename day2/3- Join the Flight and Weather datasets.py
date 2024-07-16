# Databricks notebook source
# MAGIC %md # Join the Flight and Weather datasets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "")

# COMMAND ----------

storage_account = getArgument("storage_account")
container_name = getArgument("container_name")
print (storage_account)
print (container_name)

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeop&st=2024-07-15T09:02:04Z&se=2024-08-01T17:02:04Z&spr=https&sv=2022-11-02&sr=c&sig=H4C7vXC7cDFZI8hdxZBGjrD12DYU1pNgy1RfFxeXm2I%3D")

# COMMAND ----------

# MAGIC %md With both datasets ready, we want to join them together so that we can associate historical flight delays with the weather data at departure time.

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     d.OriginAirportCode,
# MAGIC     d.Month, d.DayofMonth, d.CRSDepHour, d.DayOfWeek,
# MAGIC     d.Carrier, d.DestAirportCode, d.DepDel15, w.WindSpeed,
# MAGIC     w.SeaLevelPressure, w.HourlyPrecip
# MAGIC FROM flight_delay_silver d
# MAGIC INNER JOIN 
# MAGIC     flight_weather_silver w ON
# MAGIC                  d.OriginAirportCode = w.AirportCode AND
# MAGIC                  d.Month = w.Month AND
# MAGIC                  d.DayofMonth = w.Day AND
# MAGIC                  d.CRSDepHour = w.Hour

# COMMAND ----------

dfFlightDelaysWithWeather = _sqldf

# COMMAND ----------

# MAGIC %md Now let's take a look at the combined data.

# COMMAND ----------

display(dfFlightDelaysWithWeather)

# COMMAND ----------

# MAGIC %md Write the combined dataset to a new persistent global table.

# COMMAND ----------

#dfFlightDelaysWithWeather.write.mode("overwrite").save("/mnt/sparkcontainer/Gold/flight_delays_with_weather")

dfFlightDelaysWithWeather.write.mode("overwrite").option("header","true").\
csv(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/gold/FlightDelayWithWeather/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS flight_delays_with_weather
# MAGIC USING CSV LOCATION "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/gold/FlightDelayWithWeather/"

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables
