# Health Canada – Product Monograph Monitor

Ce projet permet de surveiller automatiquement les dates de mise à jour des Product Monographs (humain / vétérinaire) publiées dans la Drug Product Database (DPD) de Santé Canada, à partir d’une liste de drug_code.

## Ce qu'il fait
L’objectif est de :

-détecter toute modification de date de Product Monograph
-conserver un historique audit‑ready
-produire un jeu de données exploitable dans Power BI
-permettre une exécution automatisée (GitHub Actions)

## How it runs
- Les dates sont extraites directement depuis les pages publiques de la Drug Product Database (DPD) de Santé Canada, par exemple :
https://health-products.canada.ca/dpd-bdpp/info?lang=eng&code=89926
