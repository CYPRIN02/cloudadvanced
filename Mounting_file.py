# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://conteneur-grp7@grp7.blob.core.windows.net",
mount_point = "/mnt/mount_grp7",
extra_configs = {"fs.azure.account.key.grp7.blob.core.windows.net":dbutils.secrets.get(scope = "scope-grp7", key = "secretkey-grp7")})

# COMMAND ----------

df = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/test.csv", header=True)

# COMMAND ----------

df.head()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType
# Votre DataFrame est df

# Liste des colonnes numériques
num_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType))]

# Calcul de la corrélation pour chaque paire de colonnes numériques
corr_matrix = []
for i in range(len(num_columns)):
    corr_row = []
    for j in range(len(num_columns)):
        if i <= j:
            # Calculer la corrélation
            corr_value = df.stat.corr(num_columns[i], num_columns[j])
            corr_row.append(corr_value)
        else:
            # La matrice est symétrique
            corr_row.append(corr_matrix[j][i])
    corr_matrix.append(corr_row)

# Affichage de la matrice de corrélation (en option)
for row in corr_matrix:
    print(row)


# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.types import IntegerType, DoubleType

# Calcul des corrélations
num_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, DoubleType))]

corr_matrix = []
for i in range(len(num_columns)):
    corr_row = []
    for j in range(len(num_columns)):
        if i <= j:
            corr_value = df.stat.corr(num_columns[i], num_columns[j])
            corr_row.append(corr_value)
        else:
            # Utiliser la valeur de la matrice symétrique
            corr_row.append(corr_matrix[j][i])
    corr_matrix.append(corr_row)

# Convertir en DataFrame Pandas
corr_df = pd.DataFrame(corr_matrix, columns=num_columns, index=num_columns)

# Création du heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(corr_df, annot=True, fmt=".2f", cmap='coolwarm')
plt.title('Heatmap de Corrélation')
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=45)
plt.show()


# COMMAND ----------

df.write.saveAsTable("TABLEpourAutoML")

# COMMAND ----------

from pyspark.sql.functions import col

# Votre DataFrame est df

# Liste des noms de colonnes actuels
current_column_names = df.columns

# Fonction pour nettoyer les noms de colonnes
def clean_column_name(column_name):
    # Remplacer les caractères spéciaux et les espaces par des underscores
    cleaned_name = column_name
    for char in " ,;{}()\n\t=/":
        cleaned_name = cleaned_name.replace(char, "_")
    # Remplacer les caractères diacritiques, si nécessaire
    cleaned_name = cleaned_name.replace("é", "e").replace("°", "deg")
    return cleaned_name

# Application des nouveaux noms de colonnes
new_column_names = [clean_column_name(c) for c in current_column_names]
df = df.toDF(*new_column_names)

# Affichage des nouvelles colonnes pour vérification
df.printSchema()


# COMMAND ----------

# Liste des colonnes à supprimer
columns_to_drop = [
    "Facteur_couverture_solaire_saisi",
    "Cage_d_escalier",
    "Type_generateur_froid",
    "Surface_totale_capteurs_photovoltaique",
    "Cout_chauffage_energie_ndeg2",
    "Emission_GES_chauffage_energie_ndeg2",
    "Type_energie_ndeg3",
    "Type_generateur_ndeg1_installation_ndeg2",
    "Description_generateur_chauffage_ndeg2_installation_ndeg2",
    "Facteur_couverture_solaire",
    "Qualite_isolation_plancher_haut_toit_terrase"
]

# Suppression des colonnes
df = df.drop(*columns_to_drop)

# Affichage du schéma pour vérifier
df.printSchema()


# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.types import IntegerType, DoubleType

# Calcul des corrélations
num_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, DoubleType))]

corr_matrix = []
for i in range(len(num_columns)):
    corr_row = []
    for j in range(len(num_columns)):
        if i <= j:
            corr_value = df.stat.corr(num_columns[i], num_columns[j])
            corr_row.append(corr_value)
        else:
            # Utiliser la valeur de la matrice symétrique
            corr_row.append(corr_matrix[j][i])
    corr_matrix.append(corr_row)

# Convertir en DataFrame Pandas
corr_df = pd.DataFrame(corr_matrix, columns=num_columns, index=num_columns)

# Création du heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(corr_df, annot=True, fmt=".2f", cmap='coolwarm')
plt.title('Heatmap de Corrélation')
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=45)
plt.show()
