"""
Tests de calidad de datos — se ejecutan contra Azure SQL después de la carga.
Validan reglas de negocio de NYC TLC (Taxi & Limousine Commission).
"""
import os
import pytest
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

SKIP_REASON = "Azure SQL no disponible — configura variables en .env"


def sql_available() -> bool:
    return all([
        os.getenv("AZURE_SQL_SERVER"),
        os.getenv("AZURE_SQL_DATABASE"),
        os.getenv("AZURE_SQL_USERNAME"),
        os.getenv("AZURE_SQL_PASSWORD"),
    ])


@pytest.fixture(scope="module")
def db_conn():
    import pyodbc
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('AZURE_SQL_SERVER')};"
        f"DATABASE={os.getenv('AZURE_SQL_DATABASE')};"
        f"UID={os.getenv('AZURE_SQL_USERNAME')};"
        f"PWD={os.getenv('AZURE_SQL_PASSWORD')};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15;"
    )
    conn = pyodbc.connect(conn_str)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def run_date() -> str:
    return os.getenv("RUN_DATE", str(date.today()))


@pytest.mark.skipif(not sql_available(), reason=SKIP_REASON)
class TestDataQuality:

    # ── Integridad básica ───────────────────────────────────────────────────

    def test_no_duplicate_trips(self, db_conn, run_date):
        """No deben existir viajes duplicados exactos el mismo día."""
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT pickup_datetime, dropoff_datetime, pu_location_id,
                       do_location_id, fare_amount, COUNT(*) AS cnt
                FROM dbo.nyc_taxi_trips
                WHERE CAST(pickup_datetime AS DATE) = ?
                GROUP BY pickup_datetime, dropoff_datetime,
                         pu_location_id, do_location_id, fare_amount
                HAVING COUNT(*) > 1
            ) dups
        """, run_date)
        dups = cursor.fetchone()[0]
        assert dups == 0, f"Se encontraron {dups} grupos duplicados para {run_date}"

    def test_no_null_required_columns(self, db_conn, run_date):
        required = [
            "pickup_datetime", "dropoff_datetime",
            "fare_amount", "total_amount",
            "pu_location_id", "do_location_id",
        ]
        cursor = db_conn.cursor()
        for col in required:
            cursor.execute(f"""
                SELECT COUNT(*) FROM dbo.nyc_taxi_trips
                WHERE CAST(pickup_datetime AS DATE) = ?
                  AND {col} IS NULL
            """, run_date)
            nulls = cursor.fetchone()[0]
            assert nulls == 0, f"Columna '{col}' tiene {nulls} nulls para {run_date}"

    def test_dropoff_after_pickup(self, db_conn, run_date):
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND dropoff_datetime <= pickup_datetime
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} viajes con dropoff <= pickup para {run_date}"

    # ── Reglas de negocio NYC TLC ───────────────────────────────────────────

    def test_fare_amount_positive(self, db_conn, run_date):
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND fare_amount < 0
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} viajes con fare_amount negativo"

    def test_passenger_count_valid(self, db_conn, run_date):
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND (passenger_count < 1 OR passenger_count > 6)
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} viajes con passenger_count fuera de [1,6]"

    def test_trip_distance_not_negative(self, db_conn, run_date):
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND trip_distance < 0
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} viajes con distancia negativa"

    def test_total_amount_gte_fare(self, db_conn, run_date):
        """total_amount siempre debe ser >= fare_amount."""
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND total_amount < fare_amount
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} viajes donde total < fare"

    def test_location_ids_valid(self, db_conn, run_date):
        """PU y DO location IDs deben estar en el rango válido de NYC (1-263)."""
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND (pu_location_id < 1 OR pu_location_id > 263
                OR do_location_id < 1 OR do_location_id > 263)
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} viajes con location IDs fuera del rango NYC (1-263)"

    # ── Métricas calculadas ─────────────────────────────────────────────────

    def test_trip_duration_positive(self, db_conn, run_date):
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND trip_duration_minutes IS NOT NULL
              AND trip_duration_minutes <= 0
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} viajes con duración <= 0 minutos"

    def test_speed_mph_reasonable(self, db_conn, run_date):
        """Velocidades > 200 mph se eliminaron en limpieza."""
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND speed_mph IS NOT NULL
              AND speed_mph > 200
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} viajes con speed_mph > 200 (anómalos)"

    def test_tip_pct_non_negative(self, db_conn, run_date):
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND tip_pct < 0
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} viajes con tip_pct negativo"

    # ── Agregaciones ────────────────────────────────────────────────────────

    def test_agg_trip_count_positive(self, db_conn, run_date):
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.nyc_taxi_agg_zone_hour
            WHERE report_date = ? AND trip_count <= 0
        """, run_date)
        bad = cursor.fetchone()[0]
        assert bad == 0, f"{bad} registros de agregación con trip_count <= 0"

    def test_agg_covers_all_hours(self, db_conn, run_date):
        """Para días con datos, deben haber registros en múltiples horas."""
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT COUNT(DISTINCT pickup_hour) FROM dbo.nyc_taxi_agg_zone_hour
            WHERE report_date = ?
        """, run_date)
        distinct_hours = cursor.fetchone()[0]
        assert distinct_hours >= 12, \
            f"Solo {distinct_hours} horas únicas en agg_zone_hour — esperadas >= 12"

    def test_daily_metrics_revenue_positive(self, db_conn, run_date):
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT total_revenue FROM dbo.nyc_taxi_metrics_daily
            WHERE metric_date = ?
        """, run_date)
        row = cursor.fetchone()
        if row:
            assert row[0] > 0, "El revenue diario debe ser positivo"
        else:
            pytest.skip(f"No hay métricas diarias para {run_date}")