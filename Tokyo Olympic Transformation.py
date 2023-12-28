# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "626d874c-c21f-4368-96d7-794fa10c5a25",
"fs.azure.account.oauth2.client.secret": 'xcq8Q~1lbiIDJU-4Ncg6B9j05bSEWmXjMqrPwcvu',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/36b639f4-7a0f-4e93-bbe9-939e461c17b8/oauth2/token"}


dbutils.fs.mount(
source = "abfss://projetoolympicdt@projetoolympicdata.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic2",
extra_configs = configs)
  

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/tokyoolymic2"

# COMMAND ----------

spark

# COMMAND ----------

athlete = spark.read.format("csv").option('header','true').option('inferSchema','true').load("/mnt/tokyoolymic2/raw-data/athlete.csv")
coaches = spark.read.format("csv").option('header','true').option('inferSchema','true').load("/mnt/tokyoolymic2/raw-data/coaches.csv")
medals = spark.read.format("csv").option('header','true').option('inferSchema','true').load("/mnt/tokyoolymic2/raw-data/medals2.csv")
teams = spark.read.format("csv").option('header','true').option('inferSchema','true').load("/mnt/tokyoolymic2/raw-data/teams.csv")
entriesgender = spark.read.format("csv").option('header','true').option('inferSchema','true').load("/mnt/tokyoolymic2/raw-data/entriesgender.csv")

# COMMAND ----------

athlete.show(5)

# COMMAND ----------

athlete.printSchema()

# COMMAND ----------

coaches.show(5)

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.show(5)

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show(5)

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show(5)

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

#Top 5 países com maior número de medalhas de ouros
top_gold_medal_countries = medals.orderBy('Gold', ascending=False).select('Team_Country','Gold').show(5)

# COMMAND ----------

# porcentagem de genêros por esporte 
percent_gender = entriesgender.withColumn('Percent_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn('percent_male', entriesgender['Male'] / entriesgender['Total'])

percent_gender.show()

# COMMAND ----------

athlete.repartition(1).write.mode('overwrite').option('header','true').csv("/mnt/tokyoolymic2/transformed-data/athlete.csv")
coaches.repartition(1).write.mode('overwrite').option('header','true').csv("/mnt/tokyoolymic2/transformed-data/coaches.csv")
entriesgender.repartition(1).write.mode('overwrite').option('header','true').csv("/mnt/tokyoolymic2/transformed-data/entriesgender.csv")
medals.repartition(1).write.mode('overwrite').option('header','true').csv("/mnt/tokyoolymic2/transformed-data/medals.csv")
teams.repartition(1).write.mode('overwrite').option('header','true').csv("/mnt/tokyoolymic2/transformed-data/teams.csv")

# COMMAND ----------


