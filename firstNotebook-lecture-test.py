# Databricks notebook source
import pandas as pd
# Lire le fichier CSV avec pandas
df = spark.read.csv("/mnt/mount_grp7/data/test.csv",header=True)
# Afficher les premières lignes du DataFrame
display(df) 

df = df.set_index("N°DPE")

# COMMAND ----------

from pyspark.sql import SparkSession

# Assurez-vous que la session Spark est bien initiée
spark = SparkSession.builder.appName('correlation_analysis').getOrCreate()

# Votre DataFrame est déjà défini comme `df`

# Liste des colonnes pour lesquelles vous voulez calculer la corrélation
columns = df.columns

# Calcul de la corrélation pour chaque paire de colonnes
for i in range(len(columns)):
    for j in range(i + 1, len(columns)):
        corr_value = df.stat.corr(columns[i], columns[j])
        print(f"Corrélation entre {columns[i]} et {columns[j]} : {corr_value}")


# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Calcul des corrélations
columns = df.columns
corr_matrix = []

for i in range(len(columns)):
    corr_row = []
    for j in range(len(columns)):
        if i == j:
            # Corrélation d'une variable avec elle-même est toujours 1
            corr_row.append(1.0)
        elif i < j:
            corr_row.append(df.stat.corr(columns[i], columns[j]))
        else:
            # La matrice est symétrique
            corr_row.append(corr_matrix[j][i])
    corr_matrix.append(corr_row)

# Conversion en DataFrame Pandas pour une manipulation plus facile
corr_df = pd.DataFrame(corr_matrix, columns=columns, index=columns)

# Création du heatmap
plt.figure(figsize=(12, 10))
sns.heatmap(corr_df, annot=True, cmap='coolwarm')
plt.title('Heatmap de Corrélation')
plt.show()

