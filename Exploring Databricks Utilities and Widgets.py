# Databricks notebook source
access_key = ""
# Set Azure Storage Account access key
spark.conf.set(
    "fs.azure.account.key.azuremotogpdl.blob.core.windows.net",
    access_key
    )

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.fs.ls("wasbs://demo@azuremotogpdl.blob.core.windows.net/")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@azuremotogpdl.blob.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = {"fs.azure.account.key.azuremotogpdl.blob.core.windows.net": "access_key"}
)

# COMMAND ----------

display(spark.read.csv("/mnt/demo/circuits.csv"))

# COMMAND ----------

display(spark.read.csv("wasbs://demo@azuremotogpdl.blob.core.windows.net/circuits.csv"))

# COMMAND ----------

spark

# COMMAND ----------

# Databricks Utilities (DBUtils) demo

# List files in a directory
files = dbutils.fs.ls("/databricks-datasets")
display(files)


# COMMAND ----------

# Read a file
file_content = dbutils.fs.head("/databricks-datasets/README.md", 100)
print(file_content)

# COMMAND ----------

# Create a directory
dbutils.fs.mkdirs("/tmp/demo")

# Write to a file
dbutils.fs.put("/tmp/demo/sample.txt", "Hello, Databricks!", overwrite=True)

# Display the content of the written file
written_file_content = dbutils.fs.head("/tmp/demo/sample.txt")
print(written_file_content)
