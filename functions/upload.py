"""
Stage 1 – Upload local Parquet to Azure Blob Storage (raw/ container)
Jenkins llama: python functions/upload.py --file data/yellow_tripdata_2024-01.parquet
"""
import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def get_blob_client() -> BlobServiceClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING no está definida en .env")
    return BlobServiceClient.from_connection_string(conn_str)


def upload_parquet(local_path: str, container: str = "raw") -> str:
    """
    Sube el archivo parquet a Blob Storage bajo la ruta:
        raw/nyc-taxi/YYYY/MM/DD/<filename>
    Retorna la URL del blob.
    """
    file_path = Path(local_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {local_path}")

    now = datetime.utcnow()
    blob_name = f"nyc-taxi/{now.year}/{now.month:02d}/{now.day:02d}/{file_path.name}"

    client = get_blob_client()
    container_client = client.get_container_client(container)

    # Crea el container si no existe
    try:
        container_client.create_container()
        log.info("Container '%s' creado.", container)
    except Exception:
        pass  # Ya existe

    log.info("Subiendo %s → blob://%s/%s ...", file_path.name, container, blob_name)
    with open(file_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)

    url = f"https://{client.account_name}.blob.core.windows.net/{container}/{blob_name}"
    log.info("Upload completado: %s", url)

    # Exporta la URL para que Jenkins la pase al siguiente stage
    print(f"BLOB_URL={url}")
    print(f"BLOB_NAME={blob_name}")
    return url


def main():
    parser = argparse.ArgumentParser(description="Sube parquet a Azure Blob Storage")
    parser.add_argument("--file", required=True, help="Ruta local del archivo .parquet")
    parser.add_argument("--container", default="raw", help="Nombre del container (default: raw)")
    args = parser.parse_args()

    try:
        upload_parquet(args.file, args.container)
        sys.exit(0)
    except Exception as exc:
        log.error("Error en upload: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()