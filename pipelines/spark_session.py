"""Spark session factory with S3A / MinIO configuration."""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipelines.settings import PipelineConfig


def create_spark_session(config: PipelineConfig) -> SparkSession:
    minio = config.minio
    spark = config.spark

    builder = (
        SparkSession.builder
        .master(spark.master)
        .appName(spark.app_name)
        .config("spark.hadoop.fs.s3a.endpoint", minio.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(minio.secure).lower())
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    return builder.getOrCreate()
