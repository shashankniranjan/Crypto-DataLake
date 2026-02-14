#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/shashankniranjan/Documents/New project"
source "$ROOT/.venv-airflow/bin/activate"
export AIRFLOW_HOME="$ROOT/airflow/home"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"

airflow db migrate

if ! airflow users list | rg -q '^admin\s'; then
    airflow users create \
    --username admin \
    --firstname Data \
    --lastname Admin \
    --role Admin \
    --email admin@local.dev \
    --password admin
fi

echo "Airflow initialized."
