# Databricks notebook source
print("Nettoyage des données")

# import tensorflow as tf
# import json
# import os
# os.environ["CUDA_VISIBLE_DEVICES"]="-1"
# #import sys
# #print(sys.path)

# import tensorflow as tf
# from tensorflow import keras
# import numpy as np

# import sys
# sys.path.append('C:\\Users\\hp\\AppData\\Local\\Programs\\Python\\Python311\\Lib\\site-packages')

import pandas as pd
import seaborn as sns
from collections import Counter
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
import nltk
import re
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score,log_loss
from sklearn.model_selection import train_test_split
from sklearn import preprocessing


# COMMAND ----------

#insertion
# data_test = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/test.csv", header=True)
# data_train = spark.read.options(inferSchema='True').csv("/mnt/mount_grp7/data/train.csv", header=True)
data_test = pd.read_csv("/dbfs/mnt/mount_grp7/data/test.csv")
data_train = pd.read_csv("/dbfs/mnt/mount_grp7/data/train.csv")

data_test.head()
data_train.head()

data_train = data_train.set_index("N°DPE")
data_test = data_test.set_index("N°DPE")

# Mettre à jour la liste des colonnes pertinentes
relevant_columns = [
    "Surface_habitable_desservie_par_installation_ECS",
    "Emission_GES_éclairage",
    "Conso_chauffage_dépensier_installation_chauffage_n°1",
    "Surface_habitable_logement",
    "Etiquette_DPE"
]

# Filtrer les données pour ne garder que les colonnes pertinentes
df_train = data_train[relevant_columns]
df_test = data_test[relevant_columns]

# Conversion des étiquettes de la variable cible de chaînes en entiers
le = preprocessing.LabelEncoder()
df_train['Etiquette_DPE'] = le.fit_transform(df_train['Etiquette_DPE'])
df_test['Etiquette_DPE'] = le.fit_transform(df_test['Etiquette_DPE'])

# Conversion des colonnes de type objet en entiers
df_train[df_train.select_dtypes(['object']).columns] = df_train.select_dtypes(['object']).apply(lambda x: x.astype('category').cat.codes)
df_test[df_test.select_dtypes(['object']).columns] = df_test.select_dtypes(['object']).apply(lambda x: x.astype('category').cat.codes)

# Liste des colonnes pour remplacer les NaN par la moyenne
columns_with_nan = [
    "Surface_habitable_desservie_par_installation_ECS", 
    "Conso_chauffage_dépensier_installation_chauffage_n°1", 
    "Surface_habitable_logement"
]

# Calcul et remplacement des NaN par la moyenne pour chaque colonne
for col in columns_with_nan:
    mean_train = df_train[col].mean()
    mean_test = df_test[col].mean()

    df_train[col].fillna(round(mean_train), inplace=True)
    df_test[col].fillna(round(mean_test), inplace=True)


# Sauvegarder les données d'entraînement nettoyées
df_train.to_csv("/dbfs/mnt/mount_grp7/data/train_cleaned_save.csv", index=False)

# Sauvegarder les données de test nettoyées
df_test.to_csv("/dbfs/mnt/mount_grp7/data/test_cleaned_save.csv", index=False)
