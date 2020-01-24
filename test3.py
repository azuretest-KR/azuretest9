# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://azuretest9@azuretest9.blob.core.windows.net",
mount_point = "/mnt/blobmount2",
extra_configs = {"fs.azure.account.key.azuretest9.blob.core.windows.net":dbutils.secrets.get(scope = "azuretest9", key = "azuretest9")})

# COMMAND ----------

df = spark.read.text("mnt/blobmount2/insurance.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.createOrReplaceTempView("azuretest9_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT policyID FROM azuretest9_view GROUP BY EXAMPLE_GROUP

# COMMAND ----------

import java.sql.{DriverManager, SQLWarning}

Class.forName("com.ibm.db2.jcc.DB2Driver")
val url="jdbc:db2://10.254.226.154:32051/bigsql:currentSchema=POSHR;enableSeamlessFailover=yes;enableClientAffinitiesList=yes;"
val user="hdpstage"
val password="mot7QLnkud"

val connection =
    DriverManager.getConnection(url, user, password);

# COMMAND ----------

driver = "com.ibm.db2.jcc.DB2Driver"
url = "jdbc:db2://10.254.226.154:32051/bigsql"
table = "POSHR.PROMOTION_TABLE"
user = "hdpstage"
password = "mot7QLnkud"

# COMMAND ----------

remote_table = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", table)\
  .option("user", user)\
  .option("password", password)\
  .load()