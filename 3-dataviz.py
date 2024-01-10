# Databricks notebook source
print("Visualisation des données")


#insertion
df_test_clean = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/test_cleaned_save.csv", header=True)
df_train_clean = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/train_cleaned_save.csv", header=True)

# COMMAND ----------

#POUR LE DATA TEST_CLEANDED_SAVE

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

#POUR LE DATA TRAIN_CLEANDED_SAVE

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
sns.heatmap(corr_df_train_clean, annot=True, fmt=".2f", cmap='coolwarm')
plt.title('Heatmap de Corrélation')
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=45)
plt.show()

# COMMAND ----------

print(df_test_clean.head(5))
print("--------------------")
print(df_train_clean.head(5))

# COMMAND ----------

# Conversion des DataFrames Spark en DataFrames Pandas
df_test_clean_pd = df_test_clean.toPandas()
df_train_clean_pd = df_train_clean.toPandas()


# COMMAND ----------

df_test_clean_pd['Surface_habitable_logement'].hist(bins=30)
plt.title('Distribution de la Surface Habitable du Logement')
plt.xlabel('Surface Habitable (m²)')
plt.ylabel('Fréquence')
plt.show()


# COMMAND ----------

plt.scatter(df_test_clean_pd['Surface_habitable_logement'], df_test_clean_pd['Conso_chauffage_dépensier_installation_chauffage_n°1'])
plt.title('Surface Habitable vs Consommation de Chauffage')
plt.xlabel('Surface Habitable (m²)')
plt.ylabel('Consommation de Chauffage')
plt.show()


# COMMAND ----------

# Boxplots pour chaque Étiquette DPE :
# Visualisez la distribution de 
# Conso_chauffage_dépensier_installation_chauffage_n°1 pour chaque Etiquette_DPE

sns.boxplot(x='Etiquette_DPE', y='Conso_chauffage_dépensier_installation_chauffage_n°1', data=df_test_clean_pd)
plt.title('Consommation de Chauffage par Catégorie d\'Étiquette DPE')
plt.xlabel('Étiquette DPE')
plt.ylabel('Consommation de Chauffage')
plt.show()


# COMMAND ----------

# Heatmap de Corrélation :
corr = df_test_clean_pd.corr()
sns.heatmap(corr, annot=True, cmap='coolwarm')
plt.title('Heatmap de Corrélation des Variables')
plt.show()


# COMMAND ----------

# Heatmap de Corrélation :
corr = df_train_clean_pd.corr()
sns.heatmap(corr, annot=True, cmap='coolwarm')
plt.title('Heatmap de Corrélation des Variables')
plt.show()


# COMMAND ----------

sns.countplot(x='Etiquette_DPE', data=df_test_clean_pd)
plt.title('Distribution des Étiquettes DPE')
plt.xlabel('Étiquette DPE')
plt.ylabel('Nombre de Logements')
plt.show()



# COMMAND ----------

sns.scatterplot(x='Surface_habitable_logement', y='Conso_chauffage_dépensier_installation_chauffage_n°1', data=df_test_clean_pd)
plt.title('Consommation de Chauffage vs Surface Habitable')
plt.xlabel('Surface Habitable (m²)')
plt.ylabel('Consommation de Chauffage')
plt.show()


# COMMAND ----------

sns.boxplot(x='Etiquette_DPE', y='Emission_GES_éclairage', data=df_test_clean_pd)
plt.title('Impact de l\'Éclairage sur l\'Étiquette DPE')
plt.xlabel('Étiquette DPE')
plt.ylabel('Emission GES Éclairage')
plt.show()


# COMMAND ----------

sns.scatterplot(x='Surface_habitable_desservie_par_installation_ECS', y='Conso_chauffage_dépensier_installation_chauffage_n°1', data=df_test_clean_pd)
plt.title('Surface ECS vs Consommation de Chauffage')
plt.xlabel('Surface ECS (m²)')
plt.ylabel('Consommation de Chauffage')
plt.show()

