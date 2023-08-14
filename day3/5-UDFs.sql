-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("storage_account", "")
-- MAGIC dbutils.widgets.text("container_name", "")
-- MAGIC dbutils.widgets.text("table_name", "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storage_account = getArgument("storage_account")
-- MAGIC container_name = getArgument("container_name")
-- MAGIC table_name = getArgument("table_name")
-- MAGIC file_name = table_name + ".csv"
-- MAGIC print (storage_account)
-- MAGIC print (container_name)
-- MAGIC print (file_name)
-- MAGIC print (table_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

-- COMMAND ----------

create or replace temp view dpteam(id,member) as values
(1,"goodman"),
(2,"gus"),
(3,"mike"),
(4,"saul"),
(5,"kim"),
(6,"jimmy"),
(7,"walter");

select * from dpteam

-- COMMAND ----------

create
or replace temp function func_temp(member string) returns string
return concat("Mr. ", member);


-- COMMAND ----------

create or replace temp view dpteam_view as
select id, func_temp(member) from dpteam;

select * from dpteam_view 

-- COMMAND ----------

show  functions

-- COMMAND ----------

describe function extended func

-- COMMAND ----------

create
or replace function func2(member string) returns string
return case 
when member = 'kim' then concat("Ms. ", member)
else concat("Mr. ", member)
end;


-- COMMAND ----------

create or replace temp view dpteam_view_2 as
select id, func2(member) from dpteam;

select * from dpteam_view_2 

-- COMMAND ----------


