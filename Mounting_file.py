# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://conteneur-grp7@grp7.blob.core.windows.net",
mount_point = "/mnt/mount_grp7",
extra_configs = {"fs.azure.account.key.grp7.blob.core.windows.net":dbutils.secrets.get(scope = "scope-grp7", key = "secretkey-grp7")})

# COMMAND ----------

#df = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/test.csv", header=True)
df_test_clean = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/test_cleaned.csv", header=True)
df_train_clean = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/train_cleaned.csv", header=True)

# COMMAND ----------

#df.head()
df_test_clean.head()
df_train_clean.head()

# COMMAND ----------

# # #C'est pour le DATA SET TEST non clean
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
# from pyspark.sql.types import IntegerType, DoubleType

# # Calcul des corrélations
# num_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, DoubleType))]

# corr_matrix = []
# for i in range(len(num_columns)):
#     corr_row = []
#     for j in range(len(num_columns)):
#         if i <= j:
#             corr_value = df.stat.corr(num_columns[i], num_columns[j])
#             corr_row.append(corr_value)
#         else:
#             # Utiliser la valeur de la matrice symétrique
#             corr_row.append(corr_matrix[j][i])
#     corr_matrix.append(corr_row)

# # Convertir en DataFrame Pandas
# corr_df = pd.DataFrame(corr_matrix, columns=num_columns, index=num_columns)

# # Création du heatmap
# plt.figure(figsize=(10, 8))
# sns.heatmap(corr_df, annot=True, fmt=".2f", cmap='coolwarm')
# plt.title('Heatmap de Corrélation')
# plt.xticks(rotation=45, ha='right')
# plt.yticks(rotation=45)
# plt.show()




# COMMAND ----------

#POUR LE DATA TEST_CLEANDED

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.types import IntegerType, DoubleType

# Calcul des corrélations
num_columns = [field.name for field in df_test_clean.schema.fields if isinstance(field.dataType, (IntegerType, DoubleType))]

corr_matrix = []
for i in range(len(num_columns)):
    corr_row = []
    for j in range(len(num_columns)):
        if i <= j:
            corr_value = df_test_clean.stat.corr(num_columns[i], num_columns[j])
            corr_row.append(corr_value)
        else:
            # Utiliser la valeur de la matrice symétrique
            corr_row.append(corr_matrix[j][i])
    corr_matrix.append(corr_row)

# Convertir en DataFrame Pandas
corr_df_test_clean = pd.DataFrame(corr_matrix, columns=num_columns, index=num_columns)

# Création du heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(corr_df_test_clean, annot=True, fmt=".2f", cmap='coolwarm')
plt.title('Heatmap de Corrélation')
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=45)
plt.show()


# COMMAND ----------

#POUR LE DATA TRAIN_CLEANDED

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.types import IntegerType, DoubleType

# Calcul des corrélations
num_columns = [field.name for field in df_train_clean.schema.fields if isinstance(field.dataType, (IntegerType, DoubleType))]

corr_matrix = []
for i in range(len(num_columns)):
    corr_row = []
    for j in range(len(num_columns)):
        if i <= j:
            corr_value = df_train_clean.stat.corr(num_columns[i], num_columns[j])
            corr_row.append(corr_value)
        else:
            # Utiliser la valeur de la matrice symétrique
            corr_row.append(corr_matrix[j][i])
    corr_matrix.append(corr_row)

# Convertir en DataFrame Pandas
corr_df_train_clean = pd.DataFrame(corr_matrix, columns=num_columns, index=num_columns)

# Création du heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(df_train_clean, annot=True, fmt=".2f", cmap='coolwarm')
plt.title('Heatmap de Corrélation')
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=45)
plt.show()


# COMMAND ----------

#df.write.saveAsTable("TABLEpourAutoMLtest")
df_test_clean.write.saveAsTable("TABLEpourAutoMLtestclean")
df_test_clean.write.saveAsTable("TABLEpourAutoMLtrainclean")
