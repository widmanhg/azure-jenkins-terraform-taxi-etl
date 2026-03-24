"""
Stage 2 – Trigger Databricks Job y espera hasta completar.
Jenkins llama:
    python functions/run_databricks.py \
        --blob-name nyc-taxi/2024/01/15/yellow_tripdata_2024-01.parquet \
        --job-name nyc-taxi-etl
"""
import os
import sys
import time
import argparse
import logging

import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

POLL_INTERVAL_SEC = 30
MAX_WAIT_SEC = 7200  # 2 horas máximo


def get_headers() -> dict:
    token = os.getenv("DATABRICKS_TOKEN")
    if not token:
        raise EnvironmentError("DATABRICKS_TOKEN no está definida en .env")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def get_base_url() -> str:
    host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    if not host:
        raise EnvironmentError("DATABRICKS_HOST no está definida en .env")
    return host


def get_or_create_job(job_name: str) -> int:
    """Busca el job por nombre y retorna su job_id."""
    base_url = get_base_url()
    headers = get_headers()

    resp = requests.get(f"{base_url}/api/2.1/jobs/list", headers=headers, timeout=30)
    resp.raise_for_status()

    for job in resp.json().get("jobs", []):
        if job.get("settings", {}).get("name") == job_name:
            log.info("Job encontrado: '%s' (id=%s)", job_name, job["job_id"])
            return job["job_id"]

    raise ValueError(
        f"Job '{job_name}' no encontrado en Databricks. "
        "Súbelo primero con databricks/job_config.json"
    )


def trigger_job(job_id: int, blob_name: str, blob_container: str) -> int:
    """Lanza el job y retorna el run_id."""
    base_url = get_base_url()
    headers = get_headers()

    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    abfss_path = (
        f"abfss://{blob_container}@{storage_account}.dfs.core.windows.net/{blob_name}"
    )

    payload = {
        "job_id": job_id,
        "notebook_params": {
            "input_path": abfss_path,
            "output_container": "processed",
            "run_date": blob_name.split("/")[2] + "-" + blob_name.split("/")[3] + "-" + blob_name.split("/")[4]
            if len(blob_name.split("/")) >= 5 else "2024-01-15",
        },
    }

    resp = requests.post(
        f"{base_url}/api/2.1/jobs/run-now",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    run_id = resp.json()["run_id"]
    log.info("Job lanzado. run_id=%s | input=%s", run_id, abfss_path)
    return run_id


def wait_for_run(run_id: int) -> str:
    """Espera polling hasta que el run termine. Retorna el estado final."""
    base_url = get_base_url()
    headers = get_headers()
    elapsed = 0

    while elapsed < MAX_WAIT_SEC:
        resp = requests.get(
            f"{base_url}/api/2.1/jobs/runs/get",
            headers=headers,
            params={"run_id": run_id},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        life_cycle = data["state"]["life_cycle_state"]
        result_state = data["state"].get("result_state", "")

        log.info(
            "run_id=%s | lifecycle=%s | result=%s | elapsed=%ds",
            run_id, life_cycle, result_state, elapsed,
        )

        if life_cycle == "TERMINATED":
            return result_state  # SUCCESS / FAILED / CANCELED

        if life_cycle in ("INTERNAL_ERROR", "SKIPPED"):
            return life_cycle

        time.sleep(POLL_INTERVAL_SEC)
        elapsed += POLL_INTERVAL_SEC

    return "TIMEOUT"


def main():
    parser = argparse.ArgumentParser(description="Lanza y monitorea un Databricks Job")
    parser.add_argument("--blob-name", required=True, help="Ruta del blob (sin container)")
    parser.add_argument("--blob-container", default="raw", help="Container de origen")
    parser.add_argument("--job-name", default="nyc-taxi-etl", help="Nombre del job en Databricks")
    args = parser.parse_args()

    try:
        job_id = get_or_create_job(args.job_name)
        run_id = trigger_job(job_id, args.blob_name, args.blob_container)
        result = wait_for_run(run_id)

        if result == "SUCCESS":
            log.info("Job completado exitosamente.")
            print(f"DATABRICKS_RUN_ID={run_id}")
            print(f"DATABRICKS_STATUS={result}")
            sys.exit(0)
        else:
            log.error("Job terminó con estado: %s", result)
            sys.exit(1)

    except Exception as exc:
        log.error("Error en run_databricks: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()