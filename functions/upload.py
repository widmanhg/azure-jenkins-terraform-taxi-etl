"""
Stage 1 – Upload local Parquet to Azure Blob Storage (raw container)

Uso:
python functions/upload.py --file data/yellow_tripdata_2024-01.parquet
"""

import os
import sys
import argparse
import logging
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger(__name__)


# -------------------------------------------------
# Blob client
# -------------------------------------------------

def get_blob_client():

    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not conn_str:
        raise EnvironmentError(
            "AZURE_STORAGE_CONNECTION_STRING no está definida"
        )

    return BlobServiceClient.from_connection_string(
        conn_str,
        connection_timeout=300,
        read_timeout=300,
    )


# -------------------------------------------------
# Build blob name
# -------------------------------------------------

def build_blob_name(file_path: Path) -> str:
    """
    Convierte:

    yellow_tripdata_2024-01.parquet

    en

    nyc-taxi/2024/01/01/yellow_tripdata_2024-01.parquet

    Si no tiene fecha → usa default (para tests)
    """

    name = file_path.stem

    year = "2024"
    month = "01"
    day = "01"

    parts = name.split("_")

    try:
        if len(parts) >= 3:
            ym = parts[2]
            year, month = ym.split("-")
    except Exception:
        pass

    blob_name = (
        f"nyc-taxi/"
        f"{year}/"
        f"{month}/"
        f"{day}/"
        f"{file_path.name}"
    )

    return blob_name


# -------------------------------------------------
# Upload
# -------------------------------------------------

def upload_parquet(local_path: str, container: str = "raw") -> str:

    file_path = Path(local_path)

    if not file_path.exists():
        raise FileNotFoundError(local_path)

    blob_name = build_blob_name(file_path)

    client = get_blob_client()

    container_client = client.get_container_client(container)

    # crear container si no existe
    try:
        container_client.create_container()
        log.info("Container creado")
    except Exception:
        pass

    log.info(
        "Subiendo %s → %s/%s",
        file_path.name,
        container,
        blob_name,
    )

    with open(file_path, "rb") as data:

        container_client.upload_blob(
            name=blob_name,
            data=data,
            overwrite=True,
            max_concurrency=4,
        )

    url = (
        f"https://{client.account_name}.blob.core.windows.net/"
        f"{container}/{blob_name}"
    )

    log.info("Upload completado: %s", url)

    # Jenkins usa esto
    print(f"BLOB_URL={url}")
    print(f"BLOB_NAME={blob_name}")

    return url


# -------------------------------------------------
# Main
# -------------------------------------------------

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--file",
        required=True,
    )

    parser.add_argument(
        "--container",
        default="raw",
    )

    args = parser.parse_args()

    try:

        upload_parquet(
            args.file,
            args.container,
        )

        sys.exit(0)

    except Exception as e:

        log.error("Error upload: %s", e)

        sys.exit(1)


if __name__ == "__main__":
    main()