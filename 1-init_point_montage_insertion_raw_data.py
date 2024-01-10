# Databricks notebook source
print("Initialisation point de montage et insertion raw data")

# COMMAND ----------

#unmount previous mount point if necessary
if any(mount.mountPoint == "/mnt/mount_grp7" for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount("/mnt/mount_grp7")

#mount point initialization
dbutils.fs.mount(
  source = "wasbs://conteneur-grp7@grp7.blob.core.windows.net",
  mount_point = "/mnt/mount_grp7",
  extra_configs = {"fs.azure.account.key.grp7.blob.core.windows.net":dbutils.secrets.get(scope = "scope-grp7", key = "secretkey-grp7")})

# COMMAND ----------


#insertion
df_test_clean = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/test.csv", header=True)
df_train_clean = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/train.csv", header=True)

df_test_clean.head()
df_train_clean.head()
