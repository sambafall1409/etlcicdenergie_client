# Windpower ETL – Projet Data Engineering avec CI/CD Databricks

Ce projet vise à concevoir un pipeline ETL pour le traitement de données liées à la production d'énergie éolienne. Il utilise Databricks Asset Bundle et GitHub Actions pour automatiser le déploiement et l'exécution.

## Structure du projet

- `src/` : scripts de traitement et transformation des données
- `notebooks/` : notebooks Databricks intégrés au bundle
- `resources/` : configuration du job Databricks
- `deploy.yml` : pipeline CI/CD (GitHub Actions)
- `databricks.yml` : définition du bundle Databricks

## Déploiement avec Databricks CLI

```bash
databricks configure
databricks bundle deploy --target dev
databricks bundle deploy --target prod
databricks bundle run --job windpoweretl_job --target prod
