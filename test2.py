# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://azuretest9@azuretest9.blob.core.windows.net",
mount_point = "/mnt/blobmount",
extra_configs = {"fs.azure.account.key.azuretest9.blob.core.windows.net":dbutils.secrets.get(scope = "azuretest9", key = "azuretest9")})

# COMMAND ----------

df = spark.read.text("mnt/blobmount/insurance.csv")

# COMMAND ----------

df.show()