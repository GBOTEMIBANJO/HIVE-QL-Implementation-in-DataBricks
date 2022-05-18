-- Databricks notebook source
CREATE EXTERNAL TABLE IF NOT EXISTS clinicaltrial_2021(
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
location "/FileStore/tables/clinicaltrial_2021"

-- COMMAND ----------

select * from clinicaltrial_2021 limit 7

-- COMMAND ----------

select distinct count (*) from clinicaltrial_2021

-- COMMAND ----------

select Type,count(Type) from clinicaltrial_2021
group by Type
order by count(Type)desc

-- COMMAND ----------

SELECT disease,count(*)
FROM (select explode(split(conditions,',')) AS disease
      FROM clinicaltrial_2021)
where disease != ''
GROUP BY disease
ORDER BY count(*) DESC
LIMIT 5

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS mesh(
term string,
tree string)
row format delimited
fields terminated by ","
location "/FileStore/tables/mesh"

-- COMMAND ----------

SELECT * FROM mesh LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #QUESTION 4

-- COMMAND ----------

SELECT REPLACE (substring(tree,1,3),'"',''),count(disease)
FROM mesh
INNER JOIN (SELECT explode (split(conditions,',')) AS disease
      FROM clinicaltrial_2021)
ON term = disease
where disease != ''
GROUP BY REPLACE (substring(tree,1, 3), '"','')
ORDER BY count(disease) DESC
limit 5


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

SELECT * FROM pharmacy LIMIT 10

-- COMMAND ----------

SELECT sponsor,count(id)
FROM clinicaltrial_2021
left join pharmacy
ON REPLACE(parent_company,'"',"") = sponsor
WHERE regexp_replace(parent_company,'""',"")IS NULL
GROUP BY sponsor
ORDER BY count(id) DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #question 6

-- COMMAND ----------

SELECT substring(completion, 1,3),count (substring(completion, 1,3))
FROM clinicaltrial_2021 
WHERE status= "Completed" AND substring(completion,5,4)= 2021
GROUP BY substring (completion,1,3)

-- COMMAND ----------


