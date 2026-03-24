"""
Tests unitarios para las transformaciones del ETL de NYC Taxi.
No requieren conexión a Azure — usan datos sintéticos en pandas.
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta


# ─── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def sample_trips():
    """Crea un DataFrame de viajes sintéticos válidos."""
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    return pd.DataFrame({
        "pickup_datetime":   [base_time, base_time + timedelta(hours=1), base_time + timedelta(hours=2)],
        "dropoff_datetime":  [base_time + timedelta(minutes=30), base_time + timedelta(hours=1, minutes=45), base_time + timedelta(hours=2, minutes=20)],
        "vendor_id":         [1, 2, 1],
        "passenger_count":   [1, 2, 3],
        "trip_distance":     [5.2, 10.1, 3.4],
        "pu_location_id":    [161, 230, 132],  # 132 = JFK
        "do_location_id":    [4, 45, 87],
        "payment_type":      [1, 2, 1],
        "fare_amount":       [18.5, 32.0, 12.0],
        "tip_amount":        [3.7, 6.4, 0.0],
        "total_amount":      [25.3, 42.0, 15.0],
        "extra":             [0.5, 0.5, 0.5],
        "mta_tax":           [0.5, 0.5, 0.5],
        "tolls_amount":      [0.0, 0.0, 0.0],
        "congestion_surcharge": [2.5, 2.5, 2.5],
        "airport_fee":       [0.0, 0.0, 1.25],
    })


@pytest.fixture
def dirty_trips(sample_trips):
    """Añade filas inválidas al DataFrame limpio."""
    dirty = sample_trips.copy()
    # Fila con distancia negativa
    dirty.loc[len(dirty)] = dirty.iloc[0].copy()
    dirty.loc[len(dirty)-1, "trip_distance"] = -1.0
    # Fila con tarifa negativa
    dirty.loc[len(dirty)] = dirty.iloc[0].copy()
    dirty.loc[len(dirty)-1, "fare_amount"] = -5.0
    # Fila con dropoff antes que pickup
    dirty.loc[len(dirty)] = dirty.iloc[0].copy()
    dirty.loc[len(dirty)-1, "dropoff_datetime"] = dirty.iloc[0]["pickup_datetime"] - timedelta(minutes=10)
    # Fila con pasajeros fuera de rango
    dirty.loc[len(dirty)] = dirty.iloc[0].copy()
    dirty.loc[len(dirty)-1, "passenger_count"] = 0
    return dirty


# ─── Helpers locales que replican la lógica del notebook ───────────────────

def apply_clean_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Replica los filtros de calidad del notebook (en pandas para tests)."""
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
    """Replica el cálculo de métricas del notebook."""
    df = df.copy()
    df["trip_duration_minutes"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds() / 60.0

    df["speed_mph"] = df.apply(
        lambda r: r["trip_distance"] / (r["trip_duration_minutes"] / 60.0)
        if r["trip_duration_minutes"] > 0 else None,
        axis=1,
    )
    df["fare_per_mile"] = df.apply(
        lambda r: r["fare_amount"] / r["trip_distance"] if r["trip_distance"] > 0 else None,
        axis=1,
    )
    df["tip_pct"] = df.apply(
        lambda r: r["tip_amount"] / r["fare_amount"] * 100 if r["fare_amount"] > 0 else 0.0,
        axis=1,
    )
    airport_zones = {1, 132, 138}
    df["is_airport_trip"] = (
        df["pu_location_id"].isin(airport_zones) | df["do_location_id"].isin(airport_zones)
    )
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df["pickup_date"] = df["pickup_datetime"].dt.date
    return df


# ─── Tests de limpieza ──────────────────────────────────────────────────────

class TestCleaning:

    def test_clean_keeps_valid_rows(self, sample_trips):
        result = apply_clean_filters(sample_trips)
        assert len(result) == len(sample_trips), "Debería conservar todas las filas válidas"

    def test_clean_removes_negative_distance(self, dirty_trips):
        result = apply_clean_filters(dirty_trips)
        assert (result["trip_distance"] >= 0).all(), "No debe haber distancias negativas"

    def test_clean_removes_negative_fare(self, dirty_trips):
        result = apply_clean_filters(dirty_trips)
        assert (result["fare_amount"] >= 0).all(), "No debe haber tarifas negativas"

    def test_clean_removes_inverted_times(self, dirty_trips):
        result = apply_clean_filters(dirty_trips)
        assert (result["dropoff_datetime"] > result["pickup_datetime"]).all(), \
            "dropoff debe ser siempre mayor que pickup"

    def test_clean_removes_invalid_passengers(self, dirty_trips):
        result = apply_clean_filters(dirty_trips)
        assert result["passenger_count"].between(1, 6).all(), \
            "Pasajeros deben estar entre 1 y 6"

    def test_clean_dirty_removes_four_rows(self, dirty_trips, sample_trips):
        dirty_count = len(dirty_trips)
        clean_count = len(apply_clean_filters(dirty_trips))
        removed = dirty_count - clean_count
        assert removed == 4, f"Se esperaban 4 filas removidas, se removieron {removed}"

    def test_clean_no_nulls_in_critical_columns(self, sample_trips):
        result = apply_clean_filters(sample_trips)
        critical = ["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"]
        for col in critical:
            assert result[col].notna().all(), f"Columna {col} tiene nulls"


# ─── Tests de métricas ──────────────────────────────────────────────────────

class TestMetrics:

    def test_duration_is_positive(self, sample_trips):
        result = compute_metrics(sample_trips)
        assert (result["trip_duration_minutes"] > 0).all(), "La duración debe ser positiva"

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
        assert (valid_speeds <= 200).all(), "Velocidades > 200 mph son anómalas"
        assert (valid_speeds > 0).all(), "Velocidades deben ser positivas"

    def test_tip_pct_range(self, sample_trips):
        result = compute_metrics(sample_trips)
        assert (result["tip_pct"] >= 0).all(), "tip_pct no puede ser negativo"

    def test_airport_trip_detection(self, sample_trips):
        result = compute_metrics(sample_trips)
        # pu_location_id=132 (JFK) en la fila 2 debe marcarse como airport
        assert result.loc[result["pu_location_id"] == 132, "is_airport_trip"].all(), \
            "JFK (132) debe detectarse como airport trip"

    def test_non_airport_trip(self, sample_trips):
        result = compute_metrics(sample_trips)
        # pu=161, do=4 → no aeropuerto
        row = result[(result["pu_location_id"] == 161) & (result["do_location_id"] == 4)]
        assert not row["is_airport_trip"].all(), "Zona 161→4 no debe ser airport trip"

    def test_fare_per_mile_positive(self, sample_trips):
        result = compute_metrics(sample_trips)
        valid = result["fare_per_mile"].dropna()
        assert (valid > 0).all(), "fare_per_mile debe ser positivo"

    def test_pickup_hour_in_range(self, sample_trips):
        result = compute_metrics(sample_trips)
        assert result["pickup_hour"].between(0, 23).all(), "pickup_hour debe estar entre 0-23"


# ─── Tests de agregaciones ──────────────────────────────────────────────────

class TestAggregations:

    def test_zone_hour_group_count(self, sample_trips):
        df = compute_metrics(apply_clean_filters(sample_trips))
        agg = df.groupby(["pickup_date", "pu_location_id", "pickup_hour"]).agg(
            trip_count=("trip_distance", "count"),
            avg_fare=("fare_amount", "mean"),
        ).reset_index()
        assert len(agg) > 0, "La agregación zona/hora no debe estar vacía"
        assert (agg["trip_count"] > 0).all(), "Todos los grupos deben tener al menos 1 viaje"

    def test_daily_totals_match_raw(self, sample_trips):
        """La suma de viajes en la agregación debe igual al total de filas limpias."""
        df_clean = apply_clean_filters(sample_trips)
        df = compute_metrics(df_clean)
        agg = df.groupby("pickup_date").agg(total_trips=("trip_distance", "count")).reset_index()
        assert agg["total_trips"].sum() == len(df_clean), \
            "La suma de viajes por día debe coincidir con el total de filas"


# ─── Tests de reglas de negocio NYC Taxi ───────────────────────────────────

class TestBusinessRules:

    def test_payment_type_valid_values(self, sample_trips):
        """payment_type debe ser 1-6 según NYC TLC."""
        valid_types = {1, 2, 3, 4, 5, 6}
        assert set(sample_trips["payment_type"].unique()).issubset(valid_types), \
            "payment_type tiene valores inválidos"

    def test_total_amount_consistency(self, sample_trips):
        """total_amount >= fare_amount (hay extras)."""
        assert (sample_trips["total_amount"] >= sample_trips["fare_amount"]).all(), \
            "total_amount siempre debe ser >= fare_amount"

    def test_vendor_id_valid(self, sample_trips):
        """vendor_id solo puede ser 1 o 2 en yellow taxi."""
        assert set(sample_trips["vendor_id"].unique()).issubset({1, 2}), \
            "vendor_id debe ser 1 o 2"