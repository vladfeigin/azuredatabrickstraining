-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # SQL UDFs
-- MAGIC

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

-- MAGIC %md ## User-Defined Functions
-- MAGIC
-- MAGIC User Defined Functions (UDFs) in Spark SQL allow you to register custom SQL logic as functions in a database, making these methods reusable anywhere SQL can be run on Databricks. These functions are registered natively in SQL and maintain all of the optimizations of Spark when applying custom logic to large datasets.
-- MAGIC
-- MAGIC At minimum, creating a SQL UDF requires a function name, optional parameters, the type to be returned, and some custom logic.
-- MAGIC

-- COMMAND ----------

create or replace 
function add_title(member string) returns string
return concat("Mr. ", member);


-- COMMAND ----------

create or replace temp view dpteam_view as
select id, add_title(member) from dpteam;

select * from dpteam_view 

-- COMMAND ----------

show  functions

-- COMMAND ----------

describe function extended add_title

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SQL user-defined functions:
-- MAGIC - Persist between execution environments (which can include notebooks, DBSQL queries, and jobs).
-- MAGIC - Exist as objects in the metastore and are governed by the same Table ACLs as databases, tables, or views.
-- MAGIC
-- MAGIC
-- MAGIC Combining SQL UDFs with control flow in the form of **`CASE`** / **`WHEN`** clauses provides optimized execution for control flows within SQL workloads. 
-- MAGIC The standard SQL syntactic construct **`CASE`** / **`WHEN`** allows the evaluation of multiple conditional statements with alternative outcomes based on table contents.
-- MAGIC

-- COMMAND ----------

create
or replace function update_title(member string) returns string
return case 
when member = 'kim' then concat("Ms. ", member)
else concat("Mr. ", member)
end;


-- COMMAND ----------

create or replace temp view dpteam_view_updated as
select id, update_title(member) from dpteam;

select * from dpteam_view_updated

-- COMMAND ----------


