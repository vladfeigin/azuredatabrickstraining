-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ##### Define notebook variables with widgets
-- MAGIC
-- MAGIC Widgets is a great way to use variables in the notebook session across all cells

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("storage_account", "")
-- MAGIC dbutils.widgets.text("container_name", "")
-- MAGIC dbutils.widgets.text("table_name", "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storage_account = getArgument("storage_account")
-- MAGIC container_name = getArgument("container_name")
-- MAGIC table_name = getArgument("table_name","xxxx")
-- MAGIC print (storage_account)
-- MAGIC print (container_name)
-- MAGIC print (table_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Access ADLS Using SAS token
-- MAGIC
-- MAGIC Note that the recommended way to access ADLS from Databricks is by using AAD Service Principal and the backed by Azure Key Vault Databricks Secret Scope.
-- MAGIC
-- MAGIC Here for simplicity we use SAS token.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwlmeo&st=2023-09-07T14:17:14Z&se=2023-11-30T23:17:14Z&spr=https&sv=2022-11-02&sr=c&sig=jyWEvg%2FzLmK9J%2BOxIp%2B8QSCKYpVmNPfKNcNIo68Rh6E%3D")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #List the content of ADLS folder
-- MAGIC display(dbutils.fs.ls( f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create schema and tables
-- MAGIC
-- MAGIC After we explored the data we create schemas and tables in order to work with the data as we get used to work with regular data bases
-- MAGIC
-- MAGIC Note that the location of a schema (database), in example below is in the default location under dbfs:/user/hive/warehouse/ and the schema directory is the name of the schema with the .db extension
-- MAGIC
-- MAGIC However you can specify schema (database) location: `CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_custom_location LOCATION '${working_dir}/${da.schema_name}.db'; `

-- COMMAND ----------


create schema if not exists flights_demo

-- COMMAND ----------

show schemas

-- COMMAND ----------

describe schema extended flights_demo

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create external table
-- MAGIC We create a table in `flights_demo` schema, which we created previously.
-- MAGIC
-- MAGIC Notice using ***_header_*** option, which allows to define columns names from the header

-- COMMAND ----------

use flights_demo

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ###### Note! If you run in the same workspace - Please add to the table names unique prefix, example 5 digits of your ID to avoid conflicts 

-- COMMAND ----------

-- Note we use header = "true"
-- create or replace table.. also works
create table if not exists  flights_delays_external
using csv options (
  path = 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',header = "true");
  

-- COMMAND ----------

select * from flights_delays_external

-- COMMAND ----------

show tables

-- COMMAND ----------

describe extended flights_delays_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Pay attention that this table is ***External*** and the table format is CSV. Since it's an extrernal table, the location is the original one: the Azure Data Lake Storage (ADLS) container.
-- MAGIC
-- MAGIC Also you can see that there is no history for non-delta tables.
-- MAGIC
-- MAGIC The history is supported by Delta format but this table is in CSV format.

-- COMMAND ----------

-- History works only with Delta Lake format, this table is CSV format
describe history flights_delays_external

-- COMMAND ----------

-- MAGIC %md Now we can work with this table as we get used working with regular tables...

-- COMMAND ----------

select DepDel15, count(*) as number_of_delays
from flights_delays_external
group by DepDel15

-- COMMAND ----------

-- Note distinct counts null values
select distinct (DepDel15)
from flights_delays_external

-- COMMAND ----------

-- Count distinct doesn't count null values
select count (distinct (DepDel15)) as DepDel15_Valid
from flights_delays_external

-- COMMAND ----------

describe table flights_delays_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What's wrong with this table defintion?
-- MAGIC
-- MAGIC Since the original data is in CSV format, Spark can not infer precisely the correct fields types (by default). 
-- MAGIC
-- MAGIC You can enforce schema inference also for CSV, JSON formats by setting format option : `inferSchema=true`, see below examples.
-- MAGIC
-- MAGIC However the recomended way is to create table and define the types strictly.

-- COMMAND ----------


create table if not exists flights_delays_external_typed (
  year int,
  month int,
  dayofmonth int,
  dayofweek int,
  carrier string,
  crsdeptime string,
  depdelay int,
  depdel15 int,
  crsarrTime string,
  arrdelay int,
  arrdel15 int,
  canceled smallint,
  originairportcode string,
  originairportname string,
  originlatitude float,
  originlongitude float,
  destairportcode string,
  destairportname string,
  destlatitude float,
  destlongitude float
) using csv options (
  path = 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',
  header = "true"
);

-- COMMAND ----------

-- You can use "inferSchema" = "true" option to infer types instead specifying types as we did above
create table if not exists flights_delays_external_typed_infered (
) using csv options (
  path = 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',
  header = "true",
  "inferSchema" = "true"
);

-- COMMAND ----------

describe extended  flights_delays_external_typed

-- COMMAND ----------

describe extended flights_delays_external_typed_infered

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### CTEs
-- MAGIC
-- MAGIC Spark supports CTEs - Common Table Expessions.
-- MAGIC
-- MAGIC CTEs defines a temporary result set that can be referenced multiple times in other following CTE queries 
-- MAGIC
-- MAGIC In order to define CTE - you use `WITH` clause

-- COMMAND ----------

 -- define CTE
 with flights_dep_delays (origin_airport, dep_total_delays) as (
  select
    originairportname,
    sum(depdel15) as origindelay15min
  from
    flights_delays_external_typed
  group by
    originairportname
)
-- use CTE
select
  origin_airport,
  dep_total_delays
from
  flights_dep_delays
order by dep_total_delays desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Table As Select (CTAS)
-- MAGIC
-- MAGIC Create Delta Lake tables with CTAS

-- COMMAND ----------

create
or replace table flights_delays_managed_delta as
select
  *
from
  flights_delays_external_typed

-- COMMAND ----------

describe extended flights_delays_managed_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now the table type is `Managed`.  By default, if we don't specify the output format,  the tables are created in Delta format.
-- MAGIC
-- MAGIC For managed tables, the data is copied from original location (ADLS container) to the `schema` location in dbfs (Databricks File System). 
-- MAGIC
-- MAGIC The Databricks File System (DBFS) is a distributed file system mounted into an Azure Databricks Workspace and available on Azure Databricks clusters. 
-- MAGIC
-- MAGIC If you delete a ***Managed*** table all its data will be deleted as well (not the case with `External` tables)

-- COMMAND ----------

describe detail flights_delays_managed_delta

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Let's structure of Delta table dbfs:/user/hive/warehouse/flights_delays_managed_delta
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/flights_delays_managed_delta/_delta_log"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Temporay Views #####

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that CTAS syntax doesn't support schema definition (it infers the schema from query results)
-- MAGIC
-- MAGIC The recommended way to overcome it is to create a Temporay View, define or infer automatically schema and then run CTAS (similar as we did before but we used intermediate tables rather than temp views)
-- MAGIC
-- MAGIC Temporary Viewes exist only during Spark Session.

-- COMMAND ----------

 create
or replace temp view flights_delays_temp_view(
  year int,
  month int,
  day int,
  time int,
  timezone int,
  skycondition string,
  visibility string,
  weathertype string,
  drybulbfarenheit string,
  drybulbcelsius float,
  wetbulbfarenheit float,
  wetbulbcelsius float,
  dewpointfarenheit float,
  dewpointcelsius float,
  relativehumidity int,
  windspeed int,
  winddirection int,
  valueforwindcharacter int,
  stationpressure float,
  pressuretendency int,
  pressurechange int,
  sealevelpressure float,
  recordtype string,
  hourlyprecip string,
  altimeter float,
  airportcode string,
  displayairportname string,
  latitude float,
  longitude float
) using csv options (
  header = "true",
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv"
)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Now run CTAS, which creates `managed` table from temp view. Why managed?

-- COMMAND ----------

create
or replace table flights_delays_ctas as
select
  *
from
  flights_delays_temp_view

-- COMMAND ----------

describe extended flights_delays_ctas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can create a table in the specific location in Azure Blob Storage by using ***options ('path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/new table location/')***
-- MAGIC
-- MAGIC In this case an `external` table will be created.

-- COMMAND ----------

create
or replace table flights_delays_with_path_option_ext
using delta options('path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/${table_name}/')
as
select
  *
from
  flights_delays_temp_view

-- COMMAND ----------

describe extended flights_delays_with_path_option_ext

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### Enriching data with additional meta-data

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Databricks provides many built-in functions. In this example we use: `current_timestamp()` and `curent_user` functions.
-- MAGIC
-- MAGIC There are many others, more details [databricks built-in functions](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/current_timestamp)

-- COMMAND ----------

create
or replace table flights_delays_enriched as
select
  current_timestamp() as ingestiontime,
  current_user as user,
  *
from
  flights_delays_temp_view

-- COMMAND ----------


select * from flights_delays_enriched limit 10
