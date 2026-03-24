"""
Tests unitarios para functions/upload.py, functions/run_databricks.py
y functions/validate.py.
No requieren conexión a Azure — todo se mockea con unittest.mock.
"""
import os
import sys
import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES COMPARTIDOS
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sample_trips():
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    return pd.DataFrame({
        "pickup_datetime":      [base_time, base_time + timedelta(hours=1), base_time + timedelta(hours=2)],
        "dropoff_datetime":     [base_time + timedelta(minutes=30), base_time + timedelta(hours=1, minutes=45), base_time + timedelta(hours=2, minutes=20)],
        "vendor_id":            [1, 2, 1],
        "passenger_count":      [1, 2, 3],
        "trip_distance":        [5.2, 10.1, 3.4],
        "pu_location_id":       [161, 230, 132],
        "do_location_id":       [4, 45, 87],
        "payment_type":         [1, 2, 1],
        "fare_amount":          [18.5, 32.0, 12.0],
        "tip_amount":           [3.7, 6.4, 0.0],
        "total_amount":         [25.3, 42.0, 15.0],
        "extra":                [0.5, 0.5, 0.5],
        "mta_tax":              [0.5, 0.5, 0.5],
        "tolls_amount":         [0.0, 0.0, 0.0],
        "congestion_surcharge": [2.5, 2.5, 2.5],
        "airport_fee":          [0.0, 0.0, 1.25],
    })


@pytest.fixture
def dirty_trips(sample_trips):
    dirty = sample_trips.copy()
    dirty.loc[len(dirty)] = dirty.iloc[0].copy()
    dirty.loc[len(dirty)-1, "trip_distance"] = -1.0
    dirty.loc[len(dirty)] = dirty.iloc[0].copy()
    dirty.loc[len(dirty)-1, "fare_amount"] = -5.0
    dirty.loc[len(dirty)] = dirty.iloc[0].copy()
    dirty.loc[len(dirty)-1, "dropoff_datetime"] = dirty.iloc[0]["pickup_datetime"] - timedelta(minutes=10)
    dirty.loc[len(dirty)] = dirty.iloc[0].copy()
    dirty.loc[len(dirty)-1, "passenger_count"] = 0
    return dirty


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS LOCALES (replican lógica del notebook — mismos que antes)
# ═══════════════════════════════════════════════════════════════════════════════

def apply_clean_filters(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        df["pickup_datetime"].notna()
        & df["dropoff_datetime"].notna()
        & (df["dropoff_datetime"] > df["pickup_datetime"])
        & (df["trip_distance"] >= 0)
        & (df["fare_amount"] >= 0)
        & (df["total_amount"] >= 0)
        & df["passenger_count"].between(1, 6)
        & df["pu_location_id"].notna()
        & df["do_location_id"].notna()
        & (df["trip_distance"] <= 500)
        & (df["fare_amount"] <= 1000)
    ].copy()


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["trip_duration_minutes"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds() / 60.0
    df["speed_mph"] = df.apply(
        lambda r: r["trip_distance"] / (r["trip_duration_minutes"] / 60.0)
        if r["trip_duration_minutes"] > 0 else None, axis=1,
    )
    df["fare_per_mile"] = df.apply(
        lambda r: r["fare_amount"] / r["trip_distance"] if r["trip_distance"] > 0 else None, axis=1,
    )
    df["tip_pct"] = df.apply(
        lambda r: r["tip_amount"] / r["fare_amount"] * 100 if r["fare_amount"] > 0 else 0.0, axis=1,
    )
    airport_zones = {1, 132, 138}
    df["is_airport_trip"] = (
        df["pu_location_id"].isin(airport_zones) | df["do_location_id"].isin(airport_zones)
    )
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df["pickup_date"] = df["pickup_datetime"].dt.date
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS — functions/upload.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestUpload:

    def test_get_blob_client_raises_without_env(self):
        """get_blob_client lanza EnvironmentError si falta la variable."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("AZURE_STORAGE_CONNECTION_STRING", None)
            from functions.upload import get_blob_client
            with pytest.raises(EnvironmentError, match="AZURE_STORAGE_CONNECTION_STRING"):
                get_blob_client()

    def test_upload_parquet_file_not_found(self):
        """upload_parquet lanza FileNotFoundError si el archivo no existe."""
        with patch.dict(os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net"}):
            from functions.upload import upload_parquet
            with pytest.raises(FileNotFoundError):
                upload_parquet("/nonexistent/path/file.parquet")

    def test_upload_parquet_calls_blob_upload(self, tmp_path):
        """upload_parquet llama a upload_blob con el archivo correcto."""
        fake_file = tmp_path / "yellow_tripdata_2024-01.parquet"
        fake_file.write_bytes(b"fake parquet data")

        mock_container_client = MagicMock()
        mock_blob_service = MagicMock()
        mock_blob_service.get_container_client.return_value = mock_container_client
        mock_blob_service.account_name = "testaccount"

        with patch("functions.upload.get_blob_client", return_value=mock_blob_service):
            from functions.upload import upload_parquet
            url = upload_parquet(str(fake_file), container="raw")

        mock_container_client.upload_blob.assert_called_once()
        assert "testaccount" in url
        assert "raw" in url

    def test_upload_parquet_blob_name_format(self, tmp_path):
        """El blob_name debe seguir el formato nyc-taxi/YYYY/MM/DD/<filename>."""
        fake_file = tmp_path / "yellow_tripdata_2024-01.parquet"
        fake_file.write_bytes(b"fake parquet data")

        mock_container_client = MagicMock()
        mock_blob_service = MagicMock()
        mock_blob_service.get_container_client.return_value = mock_container_client
        mock_blob_service.account_name = "testaccount"

        with patch("functions.upload.get_blob_client", return_value=mock_blob_service):
            from functions.upload import upload_parquet
            url = upload_parquet(str(fake_file), container="raw")

        call_kwargs = mock_container_client.upload_blob.call_args
        blob_name_used = call_kwargs[1]["name"] if "name" in call_kwargs[1] else call_kwargs[0][0]
        assert blob_name_used.startswith("nyc-taxi/")
        assert "yellow_tripdata_2024-01.parquet" in blob_name_used

    def test_upload_parquet_creates_container(self, tmp_path):
        """upload_parquet intenta crear el container."""
        fake_file = tmp_path / "test.parquet"
        fake_file.write_bytes(b"data")

        mock_container_client = MagicMock()
        mock_blob_service = MagicMock()
        mock_blob_service.get_container_client.return_value = mock_container_client
        mock_blob_service.account_name = "acc"

        with patch("functions.upload.get_blob_client", return_value=mock_blob_service):
            from functions.upload import upload_parquet
            upload_parquet(str(fake_file))

        mock_container_client.create_container.assert_called_once()

    def test_upload_returns_url_string(self, tmp_path):
        """upload_parquet retorna una URL string válida."""
        fake_file = tmp_path / "test.parquet"
        fake_file.write_bytes(b"data")

        mock_container_client = MagicMock()
        mock_blob_service = MagicMock()
        mock_blob_service.get_container_client.return_value = mock_container_client
        mock_blob_service.account_name = "myaccount"

        with patch("functions.upload.get_blob_client", return_value=mock_blob_service):
            from functions.upload import upload_parquet
            result = upload_parquet(str(fake_file))

        assert isinstance(result, str)
        assert result.startswith("https://")


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS — functions/run_databricks.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunDatabricks:

    def test_get_headers_raises_without_token(self):
        """get_headers lanza EnvironmentError si falta DATABRICKS_TOKEN."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DATABRICKS_TOKEN", None)
            from functions.run_databricks import get_headers
            with pytest.raises(EnvironmentError, match="DATABRICKS_TOKEN"):
                get_headers()

    def test_get_base_url_raises_without_host(self):
        """get_base_url lanza EnvironmentError si falta DATABRICKS_HOST."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DATABRICKS_HOST", None)
            from functions.run_databricks import get_base_url
            with pytest.raises(EnvironmentError, match="DATABRICKS_HOST"):
                get_base_url()

    def test_get_base_url_strips_trailing_slash(self):
        """get_base_url elimina la barra final del host."""
        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://adb-123.azuredatabricks.net/"}):
            from functions.run_databricks import get_base_url
            url = get_base_url()
        assert not url.endswith("/")
        assert url == "https://adb-123.azuredatabricks.net"

    def test_get_or_create_job_found(self):
        """get_or_create_job retorna el job_id cuando el job existe."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "jobs": [
                {"job_id": 42, "settings": {"name": "nyc-taxi-etl"}},
                {"job_id": 99, "settings": {"name": "other-job"}},
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://host", "DATABRICKS_TOKEN": "tok"}):
            with patch("functions.run_databricks.requests.get", return_value=mock_response):
                from functions.run_databricks import get_or_create_job
                job_id = get_or_create_job("nyc-taxi-etl")

        assert job_id == 42

    def test_get_or_create_job_not_found(self):
        """get_or_create_job lanza ValueError si el job no existe."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"jobs": []}
        mock_response.raise_for_status = MagicMock()

        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://host", "DATABRICKS_TOKEN": "tok"}):
            with patch("functions.run_databricks.requests.get", return_value=mock_response):
                from functions.run_databricks import get_or_create_job
                with pytest.raises(ValueError, match="no encontrado"):
                    get_or_create_job("nonexistent-job")

    def test_trigger_job_returns_run_id(self):
        """trigger_job retorna el run_id de la respuesta."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"run_id": 1234}
        mock_response.raise_for_status = MagicMock()

        with patch.dict(os.environ, {
            "DATABRICKS_HOST": "https://host",
            "DATABRICKS_TOKEN": "tok",
            "AZURE_STORAGE_ACCOUNT": "myaccount",
        }):
            with patch("functions.run_databricks.requests.post", return_value=mock_response):
                from functions.run_databricks import trigger_job
                run_id = trigger_job(job_id=42, blob_name="nyc-taxi/2024/01/15/file.parquet", blob_container="raw")

        assert run_id == 1234

    def test_wait_for_run_success(self):
        """wait_for_run retorna SUCCESS cuando lifecycle=TERMINATED y result=SUCCESS."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}
        }
        mock_response.raise_for_status = MagicMock()

        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://host", "DATABRICKS_TOKEN": "tok"}):
            with patch("functions.run_databricks.requests.get", return_value=mock_response):
                with patch("functions.run_databricks.time.sleep"):
                    from functions.run_databricks import wait_for_run
                    result = wait_for_run(run_id=1234)

        assert result == "SUCCESS"

    def test_wait_for_run_failed(self):
        """wait_for_run retorna FAILED cuando el job falla."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"}
        }
        mock_response.raise_for_status = MagicMock()

        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://host", "DATABRICKS_TOKEN": "tok"}):
            with patch("functions.run_databricks.requests.get", return_value=mock_response):
                with patch("functions.run_databricks.time.sleep"):
                    from functions.run_databricks import wait_for_run
                    result = wait_for_run(run_id=1234)

        assert result == "FAILED"

    def test_wait_for_run_internal_error(self):
        """wait_for_run retorna INTERNAL_ERROR en ese lifecycle state."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "state": {"life_cycle_state": "INTERNAL_ERROR", "result_state": ""}
        }
        mock_response.raise_for_status = MagicMock()

        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://host", "DATABRICKS_TOKEN": "tok"}):
            with patch("functions.run_databricks.requests.get", return_value=mock_response):
                with patch("functions.run_databricks.time.sleep"):
                    from functions.run_databricks import wait_for_run
                    result = wait_for_run(run_id=99)

        assert result == "INTERNAL_ERROR"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS — functions/validate.py
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# TESTS — functions/validate.py
# ═══════════════════════════════════════════════════════════════════════════════

# Mockear pyodbc a nivel de módulo ANTES de importar validate
import unittest.mock
sys.modules.setdefault("pyodbc", unittest.mock.MagicMock())

import functions.validate as validate_module


class TestValidate:

    def _make_cursor(self, return_values: list):
        cursor = MagicMock()
        cursor.fetchone.side_effect = [(v,) for v in return_values]
        return cursor

    def test_get_connection_raises_missing_vars(self):
        with patch.dict(os.environ, {}, clear=True):
            for var in ["AZURE_SQL_SERVER", "AZURE_SQL_DATABASE", "AZURE_SQL_USERNAME", "AZURE_SQL_PASSWORD"]:
                os.environ.pop(var, None)
            with pytest.raises(EnvironmentError, match="Variables faltantes"):
                validate_module.get_connection()

    def test_check_row_count_passes(self):
        cursor = self._make_cursor([5000])
        result = validate_module.check_row_count(cursor, "2024-01-15", min_rows=1000)
        assert result == 5000

    def test_check_row_count_fails(self):
        cursor = self._make_cursor([500])
        with pytest.raises(validate_module.ValidationError, match="Pocas filas"):
            validate_module.check_row_count(cursor, "2024-01-15", min_rows=1000)

    def test_check_nulls_passes(self):
        cursor = self._make_cursor([0] * 7)
        validate_module.check_nulls(cursor, "2024-01-15")

    def test_check_nulls_fails_on_null_column(self):
        cursor = self._make_cursor([0, 0, 3, 0, 0, 0, 0])
        with pytest.raises(validate_module.ValidationError, match="nulls"):
            validate_module.check_nulls(cursor, "2024-01-15")

    def test_check_business_rules_passes(self):
        cursor = self._make_cursor([0, 0, 0, 0])
        validate_module.check_business_rules(cursor, "2024-01-15")

    def test_check_business_rules_fails_negative_distance(self):
        cursor = self._make_cursor([5, 0, 0, 0])
        with pytest.raises(validate_module.ValidationError, match="trip_distance"):
            validate_module.check_business_rules(cursor, "2024-01-15")

    def test_check_aggregations_table_passes(self):
        cursor = self._make_cursor([24])
        result = validate_module.check_aggregations_table(cursor, "2024-01-15")
        assert result == 24

    def test_check_aggregations_table_fails_empty(self):
        cursor = self._make_cursor([0])
        with pytest.raises(validate_module.ValidationError, match="vacía"):
            validate_module.check_aggregations_table(cursor, "2024-01-15")

    def test_check_metrics_table_fails_empty(self):
        cursor = self._make_cursor([0])
        with pytest.raises(validate_module.ValidationError, match="vacía"):
            validate_module.check_metrics_table(cursor, "2024-01-15")

    def test_run_all_validations_success(self):
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = self._make_cursor(
            [5000] + [0]*7 + [0]*4 + [24] + [10]
        )
        with patch.object(validate_module, "get_connection", return_value=mock_conn):
            result = validate_module.run_all_validations("2024-01-15")
        assert result is True

    def test_run_all_validations_failure(self):
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = self._make_cursor([0])
        with patch.object(validate_module, "get_connection", return_value=mock_conn):
            result = validate_module.run_all_validations("2024-01-15")
        assert result is False


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS ORIGINALES — limpieza y métricas (helpers locales)
# ═══════════════════════════════════════════════════════════════════════════════

class TestCleaning:

    def test_clean_keeps_valid_rows(self, sample_trips):
        result = apply_clean_filters(sample_trips)
        assert len(result) == len(sample_trips)

    def test_clean_removes_negative_distance(self, dirty_trips):
        result = apply_clean_filters(dirty_trips)
        assert (result["trip_distance"] >= 0).all()

    def test_clean_removes_negative_fare(self, dirty_trips):
        result = apply_clean_filters(dirty_trips)
        assert (result["fare_amount"] >= 0).all()

    def test_clean_removes_inverted_times(self, dirty_trips):
        result = apply_clean_filters(dirty_trips)
        assert (result["dropoff_datetime"] > result["pickup_datetime"]).all()

    def test_clean_removes_invalid_passengers(self, dirty_trips):
        result = apply_clean_filters(dirty_trips)
        assert result["passenger_count"].between(1, 6).all()

    def test_clean_dirty_removes_four_rows(self, dirty_trips, sample_trips):
        removed = len(dirty_trips) - len(apply_clean_filters(dirty_trips))
        assert removed == 4

    def test_clean_no_nulls_in_critical_columns(self, sample_trips):
        result = apply_clean_filters(sample_trips)
        for col in ["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"]:
            assert result[col].notna().all()


class TestMetrics:

    def test_duration_is_positive(self, sample_trips):
        result = compute_metrics(sample_trips)
        assert (result["trip_duration_minutes"] > 0).all()

    def test_duration_matches_times(self, sample_trips):
        result = compute_metrics(sample_trips)
        expected = (sample_trips["dropoff_datetime"] - sample_trips["pickup_datetime"]).dt.total_seconds() / 60
        pd.testing.assert_series_equal(
            result["trip_duration_minutes"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_speed_is_reasonable(self, sample_trips):
        result = compute_metrics(sample_trips)
        valid_speeds = result["speed_mph"].dropna()
        assert (valid_speeds <= 200).all()
        assert (valid_speeds > 0).all()

    def test_tip_pct_range(self, sample_trips):
        result = compute_metrics(sample_trips)
        assert (result["tip_pct"] >= 0).all()

    def test_airport_trip_detection(self, sample_trips):
        result = compute_metrics(sample_trips)
        assert result.loc[result["pu_location_id"] == 132, "is_airport_trip"].all()

    def test_non_airport_trip(self, sample_trips):
        result = compute_metrics(sample_trips)
        row = result[(result["pu_location_id"] == 161) & (result["do_location_id"] == 4)]
        assert not row["is_airport_trip"].all()

    def test_fare_per_mile_positive(self, sample_trips):
        result = compute_metrics(sample_trips)
        assert (result["fare_per_mile"].dropna() > 0).all()

    def test_pickup_hour_in_range(self, sample_trips):
        result = compute_metrics(sample_trips)
        assert result["pickup_hour"].between(0, 23).all()


class TestAggregations:

    def test_zone_hour_group_count(self, sample_trips):
        df = compute_metrics(apply_clean_filters(sample_trips))
        agg = df.groupby(["pickup_date", "pu_location_id", "pickup_hour"]).agg(
            trip_count=("trip_distance", "count"),
            avg_fare=("fare_amount", "mean"),
        ).reset_index()
        assert len(agg) > 0
        assert (agg["trip_count"] > 0).all()

    def test_daily_totals_match_raw(self, sample_trips):
        df_clean = apply_clean_filters(sample_trips)
        agg = compute_metrics(df_clean).groupby("pickup_date").agg(
            total_trips=("trip_distance", "count")
        ).reset_index()
        assert agg["total_trips"].sum() == len(df_clean)


class TestBusinessRules:

    def test_payment_type_valid_values(self, sample_trips):
        assert set(sample_trips["payment_type"].unique()).issubset({1, 2, 3, 4, 5, 6})

    def test_total_amount_consistency(self, sample_trips):
        assert (sample_trips["total_amount"] >= sample_trips["fare_amount"]).all()

    def test_vendor_id_valid(self, sample_trips):
        assert set(sample_trips["vendor_id"].unique()).issubset({1, 2})