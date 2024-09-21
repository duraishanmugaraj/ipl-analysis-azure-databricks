# Databricks notebook source
def is_mount_exists(mount_point):
    try:
        dbutils.fs.ls(mount_point)
        return True
    except:
        return False

# COMMAND ----------

mount_containers = ['raw', 'processed', 'presentation']

# COMMAND ----------

for cointainer in mount_containers:
    mount_point = f"/mnt/ipl-data/{cointainer}/"
    if is_mount_exists(mount_point):
        dbutils.fs.unmount(mount_point)
    else:
        print(f"Mount point {mount_point} does not exists")

# COMMAND ----------

client_id = "446aaa53-df78-4af7-a7f8-f8199b0b79ef"
tenant_id = "f64cb0db-3764-49ad-88e5-053fccfd11f2"
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("ipl-scope", "ipl-secret"),
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

# COMMAND ----------

for cointainer in mount_containers:
    mount_point = f"/mnt/ipl-data/{cointainer}/"
    dbutils.fs.mount(
        source=f"abfss://{cointainer}@azureiplanalysisdl.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs,
    )
    print(f"Successfully mounted {mount_point}")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.azureiplanalysisdl.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.azureiplanalysisdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.azureiplanalysisdl.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.azureiplanalysisdl.dfs.core.windows.net", dbutils.secrets.get("ipl-scope", "ipl-secret"))
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.azureiplanalysisdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
