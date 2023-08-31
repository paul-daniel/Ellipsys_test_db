# TECHNICAL INTERVIEW

## Sujet

Vous trouverez ci-joint une base de données SQLite3 'ellipsys_test_db' contenant une table 'oa_trf_src'

On veut réduire l'espace disque pris par cette table.

Pour cela, vous devez créer une table réduite 'oa_trf_src_red' à partir de 'oa_trf_src', en remplaçant tous les champs non entiers par des identifiants entiers.

La correspondance entre identifiant et nom réel sera présentée dans une table à part, nommée 'oa_trf_src_"colonne"_lkp'.

## How to use

```bash
npm run start
```