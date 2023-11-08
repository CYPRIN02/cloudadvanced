# Databricks notebook source
import pandas as pd
# Lire le fichier CSV avec pandas
df = spark.read.csv("/mnt/mount_grp7/data/test.csv",header=True)
# Afficher les premières lignes du DataFrame
display(df)
