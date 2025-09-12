import pandas as pd
from sqlalchemy import create_engine
import os

# Configuration de la connexion à MinIO (pour lire avec Pandas)
minio_endpoint = "http://minio:9000"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"

# Configuration de la connexion à PostgreSQL
db_user = "admin"
db_password = "password"
db_host = "postgres"  # Le nom du service dans docker-compose
db_name = "airquality"
db_connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}"

def main():
    # Lire le fichier Parquet depuis MinIO
    # Note: On utilise le protocole s3fs pour que Pandas comprenne MinIO
    path = "s3://processed/air_quality_metrics/"
    
    # Il faut configurer les credentials MinIO pour s3fs
    os.environ["AWS_ACCESS_KEY_ID"] = minio_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = minio_secret_key
    os.environ["AWS_ENDPOINT_URL"] = minio_endpoint
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"  # Important pour Pandas
    os.environ["AWS_ALLOW_HTTP"] = "true"

    print("Reading Parquet files from MinIO...")
    df = pd.read_parquet(path, engine='pyarrow')
    print(f"Successfully read {len(df)} records.")

    # Se connecter à PostgreSQL
    engine = create_engine(db_connection_string)

    # Charger les données dans la table
    print("Loading data into PostgreSQL...")
    df.to_sql('air_quality_metrics', engine, if_exists='append', index=False)
    print("Data load completed successfully.")

if __name__ == "__main__":
    main()