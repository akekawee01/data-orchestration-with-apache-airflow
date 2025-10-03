# Data Orchestration with Apache Airflow

Learn how to perform **Data Orchestration** using **Apache Airflow** to build automated data pipelines for efficient data management and analysis.

## How to Run Airflow

**On Linux**, Airflow needs your user ID and group ID to set up correctly. If not, files in `dags`, `logs`, `config`, and `plugins` may be owned by the root user.  
Run these commands first:

```sh
mkdir -p ./config ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Start all Airflow services with:

```sh
docker compose up -d
```

## How to Clean Up

To stop everything, remove containers, delete volumes with database data, and clear images, run:

```sh
docker compose down --volumes --rmi all
```

docker compose --profile debug run airflow-cli backfill create --dag-id templating --from-date 2025-08-08 --to-date 2025-08-14