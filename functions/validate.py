"""
Stage 3 – Valida que los datos llegaron correctamente a Azure SQL.
Jenkins llama: python functions/validate.py --run-date 2024-01-15
"""
import os
import sys
import argparse
import logging
from datetime import date

import pyodbc
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def get_connection():
    server   = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")

    missing = [k for k, v in {
        "AZURE_SQL_SERVER": server,
        "AZURE_SQL_DATABASE": database,
        "AZURE_SQL_USERNAME": username,
        "AZURE_SQL_PASSWORD": password,
    }.items() if not v]

    if missing:
        raise EnvironmentError(f"Variables faltantes en .env: {', '.join(missing)}")

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


# ---------------------------------------------------------------------------
# Reglas de validación
# ---------------------------------------------------------------------------

class ValidationError(Exception):
    pass


def check_row_count(cursor, run_date: str, min_rows: int = 1000):
    """Verifica que haya al menos min_rows para la fecha dada."""
    cursor.execute(
        """
        SELECT COUNT(*) FROM dbo.nyc_taxi_trips
        WHERE CAST(pickup_datetime AS DATE) = ?
        """,
        run_date,
    )
    count = cursor.fetchone()[0]
    log.info("Filas para %s: %s (mínimo requerido: %s)", run_date, count, min_rows)
    if count < min_rows:
        raise ValidationError(
            f"Pocas filas para {run_date}: {count} < {min_rows}"
        )
    return count


def check_nulls(cursor, run_date: str):
    """Verifica que columnas críticas no tengan nulls."""
    critical_cols = [
        "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance",
        "fare_amount", "pu_location_id", "do_location_id",
    ]
    for col in critical_cols:
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM dbo.nyc_taxi_trips
            WHERE CAST(pickup_datetime AS DATE) = ?
              AND {col} IS NULL
            """,
            run_date,
        )
        nulls = cursor.fetchone()[0]
        if nulls > 0:
            raise ValidationError(f"Columna '{col}' tiene {nulls} nulls para {run_date}")
        log.info("Columna '%s': sin nulls.", col)


def check_business_rules(cursor, run_date: str):
    """Reglas de negocio básicas de NYC Taxi."""
    checks = [
        ("trip_distance >= 0",    "SELECT COUNT(*) FROM dbo.nyc_taxi_trips WHERE CAST(pickup_datetime AS DATE) = ? AND trip_distance < 0"),
        ("fare_amount >= 0",      "SELECT COUNT(*) FROM dbo.nyc_taxi_trips WHERE CAST(pickup_datetime AS DATE) = ? AND fare_amount < 0"),
        ("passenger_count 1-6",   "SELECT COUNT(*) FROM dbo.nyc_taxi_trips WHERE CAST(pickup_datetime AS DATE) = ? AND (passenger_count < 1 OR passenger_count > 6)"),
        ("dropoff > pickup",      "SELECT COUNT(*) FROM dbo.nyc_taxi_trips WHERE CAST(pickup_datetime AS DATE) = ? AND dropoff_datetime <= pickup_datetime"),
    ]
    for rule_name, sql in checks:
        cursor.execute(sql, run_date)
        bad = cursor.fetchone()[0]
        if bad > 0:
            raise ValidationError(f"Regla '{rule_name}' fallida: {bad} filas inválidas")
        log.info("Regla '%s': OK", rule_name)


def check_aggregations_table(cursor, run_date: str):
    """Verifica que la tabla de agregaciones zona/hora esté cargada."""
    cursor.execute(
        """
        SELECT COUNT(*) FROM dbo.nyc_taxi_agg_zone_hour
        WHERE report_date = ?
        """,
        run_date,
    )
    count = cursor.fetchone()[0]
    log.info("Agregaciones zona/hora para %s: %s filas", run_date, count)
    if count == 0:
        raise ValidationError(
            f"Tabla de agregaciones vacía para {run_date}"
        )
    return count


def check_metrics_table(cursor, run_date: str):
    """Verifica que las métricas calculadas existan."""
    cursor.execute(
        """
        SELECT COUNT(*) FROM dbo.nyc_taxi_metrics_daily
        WHERE metric_date = ?
        """,
        run_date,
    )
    count = cursor.fetchone()[0]
    log.info("Métricas diarias para %s: %s filas", run_date, count)
    if count == 0:
        raise ValidationError(
            f"Tabla de métricas vacía para {run_date}"
        )


def run_all_validations(run_date: str) -> bool:
    """Ejecuta todas las validaciones. Retorna True si todas pasan."""
    log.info("=== Iniciando validaciones para fecha: %s ===", run_date)
    conn = get_connection()
    cursor = conn.cursor()
    errors = []

    validations = [
        ("row_count",        lambda: check_row_count(cursor, run_date)),
        ("nulls",            lambda: check_nulls(cursor, run_date)),
        ("business_rules",   lambda: check_business_rules(cursor, run_date)),
        ("aggregations",     lambda: check_aggregations_table(cursor, run_date)),
        ("metrics",          lambda: check_metrics_table(cursor, run_date)),
    ]

    for name, fn in validations:
        try:
            fn()
            log.info("[PASS] %s", name)
        except ValidationError as e:
            log.error("[FAIL] %s: %s", name, e)
            errors.append(str(e))

    conn.close()

    if errors:
        log.error("=== %d validación(es) fallida(s) ===", len(errors))
        for e in errors:
            log.error("  • %s", e)
        return False

    log.info("=== Todas las validaciones pasaron ===")
    return True


def main():
    parser = argparse.ArgumentParser(description="Valida datos en Azure SQL post-ETL")
    parser.add_argument(
        "--run-date",
        default=str(date.today()),
        help="Fecha a validar (YYYY-MM-DD). Default: hoy.",
    )
    args = parser.parse_args()

    success = run_all_validations(args.run_date)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()