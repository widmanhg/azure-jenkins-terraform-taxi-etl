"""
Tests de integración — verifican conectividad real con Azure.
Se ejecutan después del ETL (Stage integration en Jenkins).
Requieren variables de entorno configuradas.
"""
import os
import pytest
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

SKIP_REASON = "Variables de entorno Azure no configuradas"


def azure_storage_available() -> bool:
    return bool(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))


def azure_sql_available() -> bool:
    return all([
        os.getenv("AZURE_SQL_SERVER"),
        os.getenv("AZURE_SQL_DATABASE"),
        os.getenv("AZURE_SQL_USERNAME"),
        os.getenv("AZURE_SQL_PASSWORD"),
    ])


def databricks_available() -> bool:
    return all([
        os.getenv("DATABRICKS_HOST"),
        os.getenv("DATABRICKS_TOKEN"),
    ])


# ─── Blob Storage ───────────────────────────────────────────────────────────

@pytest.mark.skipif(not azure_storage_available(), reason=SKIP_REASON)
class TestBlobStorage:

    def test_raw_container_exists(self):
        from azure.storage.blob import BlobServiceClient
        client = BlobServiceClient.from_connection_string(
            os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        )
        containers = [c["name"] for c in client.list_containers()]
        assert "raw" in containers, "El container 'raw' no existe en Blob Storage"

    def test_processed_container_exists(self):
        from azure.storage.blob import BlobServiceClient
        client = BlobServiceClient.from_connection_string(
            os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        )
        containers = [c["name"] for c in client.list_containers()]
        assert "processed" in containers, "El container 'processed' no existe en Blob Storage"

    def test_recent_blob_uploaded(self):
        """Verifica que existe al menos un blob en raw/nyc-taxi/ reciente."""
        from azure.storage.blob import BlobServiceClient
        client = BlobServiceClient.from_connection_string(
            os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        )
        container = client.get_container_client("raw")
        blobs = list(container.list_blobs(name_starts_with="nyc-taxi/"))
        assert len(blobs) > 0, "No hay blobs en raw/nyc-taxi/ — ¿falló el upload?"


# ─── Databricks ─────────────────────────────────────────────────────────────

@pytest.mark.skipif(not databricks_available(), reason=SKIP_REASON)
class TestDatabricks:

    def _headers(self):
        return {
            "Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN')}",
            "Content-Type": "application/json",
        }

    def _base_url(self):
        return os.getenv("DATABRICKS_HOST", "").rstrip("/")

    def test_databricks_connection(self):
        import requests
        resp = requests.get(
            f"{self._base_url()}/api/2.0/clusters/list",
            headers=self._headers(),
            timeout=10,
        )
        assert resp.status_code == 200, f"No se puede conectar a Databricks: {resp.status_code}"

    def test_job_exists(self):
        import requests
        resp = requests.get(
            f"{self._base_url()}/api/2.1/jobs/list",
            headers=self._headers(),
            timeout=10,
        )
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        job_names = [j.get("settings", {}).get("name") for j in jobs]
        assert "nyc-taxi-etl" in job_names, \
            "El job 'nyc-taxi-etl' no existe en Databricks. Créalo con job_config.json"

    def test_last_run_succeeded(self):
        """Verifica que el último run del job fue exitoso."""
        import requests
        resp = requests.get(
            f"{self._base_url()}/api/2.1/jobs/list",
            headers=self._headers(),
            timeout=10,
        )
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        job = next((j for j in jobs if j.get("settings", {}).get("name") == "nyc-taxi-etl"), None)
        if not job:
            pytest.skip("Job 'nyc-taxi-etl' no encontrado")

        runs_resp = requests.get(
            f"{self._base_url()}/api/2.1/jobs/runs/list",
            headers=self._headers(),
            params={"job_id": job["job_id"], "limit": 1},
            timeout=10,
        )
        runs_resp.raise_for_status()
        runs = runs_resp.json().get("runs", [])
        if not runs:
            pytest.skip("No hay runs previos del job")

        last_run = runs[0]
        result_state = last_run.get("state", {}).get("result_state", "")
        assert result_state == "SUCCESS", \
            f"El último run del job terminó en: {result_state}"


# ─── Azure SQL ──────────────────────────────────────────────────────────────

@pytest.mark.skipif(not azure_sql_available(), reason=SKIP_REASON)
class TestAzureSQL:

    def _get_conn(self):
        import pyodbc
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={os.getenv('AZURE_SQL_SERVER')};"
            f"DATABASE={os.getenv('AZURE_SQL_DATABASE')};"
            f"UID={os.getenv('AZURE_SQL_USERNAME')};"
            f"PWD={os.getenv('AZURE_SQL_PASSWORD')};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15;"
        )
        return pyodbc.connect(conn_str)

    def test_sql_connection(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
        conn.close()

    def test_trips_table_exists(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'nyc_taxi_trips' AND TABLE_SCHEMA = 'dbo'
        """)
        assert cursor.fetchone()[0] == 1, "La tabla dbo.nyc_taxi_trips no existe"
        conn.close()

    def test_agg_zone_hour_table_exists(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'nyc_taxi_agg_zone_hour' AND TABLE_SCHEMA = 'dbo'
        """)
        assert cursor.fetchone()[0] == 1, "La tabla dbo.nyc_taxi_agg_zone_hour no existe"
        conn.close()

    def test_metrics_daily_table_exists(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'nyc_taxi_metrics_daily' AND TABLE_SCHEMA = 'dbo'
        """)
        assert cursor.fetchone()[0] == 1, "La tabla dbo.nyc_taxi_metrics_daily no existe"
        conn.close()

    def test_trips_has_data(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dbo.nyc_taxi_trips")
        count = cursor.fetchone()[0]
        assert count > 0, "La tabla nyc_taxi_trips está vacía"
        conn.close()