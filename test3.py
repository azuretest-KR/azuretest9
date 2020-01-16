# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://azuretest9@azuretest9.blob.core.windows.net",
mount_point = "/mnt/blobmount1",
extra_configs = {"fs.azure.account.key.azuretest9.blob.core.windows.net":dbutils.secrets.get(scope = "azuretest9", key = "azuretest9")})

# COMMAND ----------

df = spark.read.text("mnt/blobmount1/insurance.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.createOrReplaceTempView("azuretest9_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT policyID FROM azuretest9_view GROUP BY EXAMPLE_GROUP