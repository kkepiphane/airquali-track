from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_utc_timestamp

def main():
    # Initialiser la session Spark avec la config pour MinIO
    spark = SparkSession.builder \
        .appName("AirQuality-Processing") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()

    # Lire les données JSON brutes depuis MinIO
    raw_data_path = "s3a://raw/openaq/*.json"
    df = spark.read.json(raw_data_path)

    # Aplatir la structure JSON complexe
    # On explose le tableau 'results' pour avoir une ligne par mesure
    exploded_df = df.withColumn("result", explode("results")).select("result.*")

    # Sélectionner et renommer les colonnes d'intérêt
    processed_df = exploded_df.select(
        col("location").alias("location_name"),
        col("city"),
        col("country"),
        col("parameter"),  # e.g., pm25, pm10, so2
        col("value"),
        col("unit"),
        from_utc_timestamp(col("lastUpdated"), "UTC").alias("last_updated_utc"),
        col("coordinates.latitude").alias("lat"),
        col("coordinates.longitude").alias("lon")
    )

    # Écrire le résultat au format Parquet dans le bucket "processed"
    output_path = "s3a://processed/air_quality_metrics/"
    processed_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("parameter") \
        .save(output_path)

    spark.stop()

if __name__ == "__main__":
    main()