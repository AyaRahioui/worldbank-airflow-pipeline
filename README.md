# Pipeline d'ingestion World Bank (Airflow + GCP)

DAG Airflow (Cloud Composer 3, europe-west9) qui récupère 5 indicateurs
(GDP, inflation, population, chômage, espérance de vie) pour MAR, TUN, DZA, EGY, FRA
depuis l'API World Bank, dépose le CSV brut sur Cloud Storage puis le charge dans BigQuery.

## Pipeline
extract_and_upload >> load_to_bigquery >> quality_check

- `extract.py` : extraction avec retries et détection des erreurs JSON renvoyées en HTTP 200
- `worldbank_gdp_ingestion.py` : DAG `worldbank_gdp_ingestion` (@weekly)
- Destination : `raw_worldbank.indicators`, partitionnée par `load_date`, clusterisée par `country_code`
- Compte de service dédié : `storage.objectUser` (bucket), `bigquery.dataEditor` (dataset), `bigquery.jobUser` (projet)
