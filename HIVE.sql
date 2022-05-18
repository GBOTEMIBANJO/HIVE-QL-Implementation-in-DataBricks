-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC if dbutils.fs.ls('/FileStore/tables/clinicaltrial_2021') ==[]:
-- MAGIC     print("Ok")
-- MAGIC else:
-- MAGIC     dbutils.fs.rm('FileStore/tables/core',True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('FileStore/tables/core')

-- COMMAND ----------

LOAD data into File
%python
dbutils.fs.cp('/FileStore/tables/clinicaltrial_2021_csv',"/FileStore/tables/core')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/core")

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS j(
Id string,
Sponsor string,
Status string,
Start string,
Completion string,
Type string,
Submission string,
Conditions string,
Interventions string)
row format delimited
fields terminated by "|"
location "/FileStore/tables/core"

-- COMMAND ----------

select * from j limit 10

-- COMMAND ----------

select count (*) from j

-- COMMAND ----------

select Type,count(Type) from j
group by Type
order by count(Type)desc

-- COMMAND ----------

SELECT disease,count(*)
FROM (select explode(split(conditions,',')) AS disease
      FROM j)
where disease != ''
GROUP BY disease
ORDER BY count(*) DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('FileStore/tables/dore')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('/FileStore/tables/mesh.csv',"/FileStore/tables/dore")

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS dore(
term string,
tree string)
row format delimited
fields terminated by ","
location "/FileStore/tables/dore"

-- COMMAND ----------

SELECT * FROM dore LIMIT 5

-- COMMAND ----------

SELECT REPLACE (substring(tree,1,3),'"',''),count(disease)
FROM dore
INNER JOIN (SELECT explode (split(conditions,',')) AS disease
      FROM j)
ON term = disease
where disease != ''
GROUP BY REPLACE (substring(tree,1, 3), '"','')
ORDER BY count(disease) DESC
limit 5


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('FileStore/tables/pharmacy')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('FileStore/tables/pharma.csv',"/FileStore/tables/pharmacy")

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS pharmacy(
Company string,
Parent_Company string,
Penalty_Amount string,
Subtraction_From_Penalty string,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting string,
Penalty_Year string,
Penalty_Date string,
Offense_Group string,
Primary_Offense string,
Secondary_Offense string,
Description string,
Level_of_Government string,
Action_Type string,
Agency string,
Civil_Criminal string,
Prosecution_Agreement string,
Court string,
Case_ID string,
Private_Litigation_Case_Title string,
Lawsuit_Resolution string,
Facility_State string,
City string,
Address string,
Zip string,
NAICS_Code string,
NAICS_Translation string,
HQ_Country_of_Parent string,
HQ_State_of_Parent string,
Ownership_Structure string,
Parent_Company_Stock_Ticker string,
Major_Industry_of_Parent string,
Specific_Industry_of_Parent string,
Info_Source string,
Notes string)
row format delimited
fields terminated by ","
location "/FileStore/tables/pharmacy"

-- COMMAND ----------

SELECT * FROM pharmacy LIMIT 5

-- COMMAND ----------

SELECT sponsor,count(id)
FROM j
left join pharmacy
ON REPLACE(parent_company,'"',"") = sponsor
WHERE regexp_replace(parent_company,'""',"")IS NULL
GROUP BY sponsor
ORDER BY count(id) DESC
LIMIT 10

-- COMMAND ----------

SELECT substring(completion, 1,3),count (substring(completion, 1,3))
FROM j 
WHERE status= "Completed" AND substring(completion,5,4)= 2019
GROUP BY substring (completion,1,3)

-- COMMAND ----------


