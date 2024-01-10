# Databricks notebook source
print("appel du modèle via l'api")


import requests
import json

# L'URL de l'API de votre modèle
url = 'https://adb-7085003793740174.14.azuredatabricks.net/model/ModelMetierCloudAdvanced/1/invocations'

# Les données que vous souhaitez envoyer au modèle pour la prédiction, formatées comme spécifié.
data = {
    "columns": [
        "Surface_habitable_desservie_par_installation_ECS",
        "Emission_GES_éclairage",
        "Conso_chauffage_dépensier_installation_chauffage_n°1",
        "Surface_habitable_logement"
    ],
    "data": [
        [53.2, 6.9, 4878.3, 53.2],
        [53.8, 7, 8664.7, 53.8],
        [31.2, 4, 3987.5, 31.2],
        [59, 0.1, 12598.4, 59],
        [51.4, 6.5, 2926.2, 51.4]
    ]
}

# Convertissez vos données en JSON
json_data = json.dumps(data)

# Ajoutez les headers nécessaires, tels que le Content-Type et l'Authorization si nécessaire
headers = {
    'Authorization': 'Bearer dapi0dd817792ba34c46be0ed638430a3018',  # Décommentez et remplacez par votre jeton d'accès si nécessaire
    'Content-Type': 'application/json',
}

# Faire la requête POST à l'API
response = requests.post(url, headers=headers, data=json_data)

# Vérifiez le code de statut de la réponse et les éventuels messages d'erreur
if response.status_code == 200:
    # Si la réponse est réussie, traitez la prédiction
    predictions = response.json()
    print("Prédictions du modèle : ", predictions)
else:
    # S'il y a une erreur, affichez le code de statut et le message d'erreur
    print("Erreur lors de l'appel de l'API : ", response.status_code)
    print(response.text)


# COMMAND ----------

import requests

url = 'https://adb-7085003793740174.14.azuredatabricks.net/model/ModelMetierCloudAdvanced/1/invocations'
token = 'dapi0dd817792ba34c46be0ed638430a3018'  # Remplacez ceci par votre jeton d'accès personnel réel.

headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
}

data = {
    "instances": [
        {
            "Surface_habitable_desservie_par_installation_ECS": 53.2,
            "Emission_GES_éclairage": 6.9,
            "Conso_chauffage_dépensier_installation_chauffage_n°1": 4878.3,
            "Surface_habitable_logement": 53.2
        },

        {
      "Surface_habitable_desservie_par_installation_ECS": 45.7,
      "Emission_GES_éclairage": 5.2,
      "Conso_chauffage_dépensier_installation_chauffage_n°1": 3200.1,
      "Surface_habitable_logement": 45.7
    },
    {
      "Surface_habitable_desservie_par_installation_ECS": 75.3,
      "Emission_GES_éclairage": 9.5,
      "Conso_chauffage_dépensier_installation_chauffage_n°1": 6000.4,
      "Surface_habitable_logement": 75.3
    },
    {
      "Surface_habitable_desservie_par_installation_ECS": 30.0,
      "Emission_GES_éclairage": 3.6,
      "Conso_chauffage_dépensier_installation_chauffage_n°1": 2000.0,
      "Surface_habitable_logement": 30.0
    }
        # ... autres instances ...
    ]
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    print(f"Ok lors de l'appel de l'API : {response.status_code}")
    print("Prédictions du modèle : ", response.json())
else:
    print(f"Erreur lors de l'appel de l'API : {response.status_code}")
    print(response.text)


# COMMAND ----------

import requests

# L'URL de l'API de votre modèle
url = 'https://adb-7085003793740174.14.azuredatabricks.net/model/ModelMetierCloudAdvanced/1/invocations'
token = 'dapi0dd817792ba34c46be0ed638430a3018'

headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
}

# Les données à envoyer pour la prédiction
data = {
    "instances": [
        {
            "Surface_habitable_desservie_par_installation_ECS": 53.2,
            "Emission_GES_éclairage": 6.9,
            "Conso_chauffage_dépensier_installation_chauffage_n°1": 4878.3,
            "Surface_habitable_logement": 53.2
        },

        {
      "Surface_habitable_desservie_par_installation_ECS": 45.7,
      "Emission_GES_éclairage": 5.2,
      "Conso_chauffage_dépensier_installation_chauffage_n°1": 3200.1,
      "Surface_habitable_logement": 45.7
    },
    {
      "Surface_habitable_desservie_par_installation_ECS": 75.3,
      "Emission_GES_éclairage": 9.5,
      "Conso_chauffage_dépensier_installation_chauffage_n°1": 6000.4,
      "Surface_habitable_logement": 75.3
    },
    {
      "Surface_habitable_desservie_par_installation_ECS": 30.0,
      "Emission_GES_éclairage": 3.6,
      "Conso_chauffage_dépensier_installation_chauffage_n°1": 2000.0,
      "Surface_habitable_logement": 30.0
    }
    ]
}

# Envoyez la requête à l'API
response = requests.post(url, headers=headers, json=data)
# Définissez une fonction pour générer un commentaire basé sur la valeur prédite
def generer_commentaire(valeur_predite):
    commentaires = {
        '0': "Très performant (A)",
        '1': "Performant (B)",
        '2': "Peu gourmand en énergie (C)",
        '3': "Consommation modérée (D)",
        '4': "Énergivore (E)",
        '5': "Très énergivore (F)",
        '6': "Extrêmement énergivore (G)"
    }
    return commentaires.get(str(valeur_predite), "Valeur inconnue")

# Vérifiez le code de statut et traitez la réponse
if response.status_code == 200:
    predictions = response.json()
    #print(predictions)
    resultats = []
    
    # Pour chaque prédiction, créez un dictionnaire avec la prédiction en tant que clé et le commentaire
    for pred_list in predictions.values():
        #print(pred_list)
        for val in pred_list:
            
            valeur_predite = str(val)  # Supposons que chaque prédiction est un entier de 0 à 6
            commentaire = generer_commentaire(valeur_predite)
            resultat = {
                'valeur_predite': valeur_predite,
                'commentaire': commentaire
            }
            
            print(f"Résultats des prédictions avec commentaires : {resultat}")
            
            resultats.append(resultat)
    
    print("\n \nRésultats des prédictions avec commentaires : \n", resultats)
else:
    print("Erreur lors de l'appel de l'API : ", response.status_code)
    print(response.text)


