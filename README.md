# Lab3 Spark - E-commerce Data Transformation

**Équipe :** 404 Not Found  

---

## 1. Description
Ce dépôt contient tous les fichiers relatifs au **Lab3 Spark**, centré sur la transformation et l'analyse des données e-commerce.  
Les objectifs principaux du lab sont :

- Manipuler des **DataFrames PySpark** pour filtrer, transformer et agréger les données.  
- Calculer des **statistiques sur les commandes et clients** (totaux, distributions, filtres par type de client).  
- Pratiquer les **actions PySpark** : `collect()`, `take()`, `foreachPartition()`.  
- Gérer des fichiers CSV et Parquet pour simuler des workflows d'analyse de données réels.

---

## 2. Structure du dépôt

```

spark-ecommerce-lab/
│
├─ lab3_transformations.py             # Script principal de transformations
├─ lab3_transformation_practice.py     # Exercices pratiques
├─ lab3_actions.py                     # Exercices sur les actions PySpark
├─ README.md                           # Ce fichier
└─ spark-data/ecommerce/               # Données e-commerce
├─ customers_parquet/              # Données clients en Parquet
└─ usa_customers/                  # Données clients CSV

````

---

## 3. Installation et Pré-requis

- **Python 3.10+**  
- **PySpark** : installer via pip  

```bash
pip install pyspark
````

* Cloner le dépôt :

```bash
git clone https://github.com/ibti12/LAB3.git
cd spark-ecommerce-lab
```

---

## 4. Exécution des scripts

* **Exemple d’exécution :**

```bash
python lab3_transformations.py
python lab3_transformation_practice.py
python lab3_actions.py
```

* Les scripts produisent :

  * Des **résumés des commandes** (montants, statuts, distribution par taille)
  * Des **filtres de clients** (Enterprise, SMB, Startup, etc.)
  * Des **exemples d’agrégations** et calculs de statistiques

---

## 5. Résultats attendus

* Un aperçu des commandes et clients via `show()`
* Calcul des totaux et statistiques des commandes
* Identification des clients Enterprise, SMB, Startup
* Génération de colonnes supplémentaires (`orderSize`, `priority`, `orderCode`) selon des règles métiers

---

## 6. Bonnes pratiques

* Éviter l’usage de `collect()` sur de gros datasets pour prévenir des **problèmes de mémoire**.
* Utiliser `show()` ou `take()` pour visualiser des échantillons.
* Toujours vérifier que les fichiers de données existent avant d’exécuter les scripts.

---

## 7. Contact

Pour toute question sur le lab :
**Équipe 404 Not Found**
Email : [votre email]

---

## 8. Lien vers le dépôt GitHub

[LAB3 Spark E-commerce](https://github.com/ibti12/LAB3)

