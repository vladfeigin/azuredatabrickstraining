# Databricks notebook source
# MAGIC %md # Join the Flight and Weather datasets

# COMMAND ----------

# MAGIC %md With both datasets ready, we want to join them together so that we can associate historical flight delays with the weather data at departure time.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     d.OriginAirportCode,
# MAGIC     d.Month, d.DayofMonth, d.CRSDepHour, d.DayOfWeek,
# MAGIC     d.Carrier, d.DestAirportCode, d.DepDel15, w.WindSpeed,
# MAGIC     w.SeaLevelPressure, w.HourlyPrecip
# MAGIC FROM flight_delays_clean d
# MAGIC INNER JOIN 
# MAGIC     flight_weather_clean w ON
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

dfFlightDelaysWithWeather.write.mode("overwrite").option("header", "true").csv("/mnt/labs-303474/Gold/flight_delays_with_weather_csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS flight_delays_with_weather;
# MAGIC 
# MAGIC CREATE TABLE flight_delays_with_weather
# MAGIC USING DELTA LOCATION '/mnt/sparkcontainer/Gold/flight_delays_with_weather'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS flight_delays_with_weather_tab;
# MAGIC 
# MAGIC CREATE TABLE flight_delays_with_weather_tab
# MAGIC USING DELTA LOCATION '/mnt/sparkcontainer/Gold/flight_delays_with_weather_csv'
