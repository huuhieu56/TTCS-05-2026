"""Spark session factory with S3A / MinIO configuration."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from pipelines.settings import PipelineConfig


def _detect_hadoop_aws_version() -> str:
    """Return the correct hadoop-aws version for the running PySpark."""
    override = os.getenv("HADOOP_AWS_VERSION")
    if override:
        return override
    try:
        import pyspark
        major = int(pyspark.__version__.split(".")[0])
        if major >= 4:
            return "3.4.1"
        return "3.3.4"
    except Exception:
        return "3.3.4"


def _detect_aws_sdk_version(hadoop_aws_ver: str) -> str:
    """Return matching aws-java-sdk-bundle version."""
    if hadoop_aws_ver.startswith("3.4"):
        return "1.12.780"
    return "1.12.262"


def create_spark_session(config):
    minio = config.minio
    spark = config.spark

    hadoop_ver = _detect_hadoop_aws_version()
    sdk_ver = _detect_aws_sdk_version(hadoop_ver)

    packages = (
        "org.apache.hadoop:hadoop-aws:{hv},"
        "com.amazonaws:aws-java-sdk-bundle:{sv},"
        "org.postgresql:postgresql:42.7.4"
    ).format(hv=hadoop_ver, sv=sdk_ver)

    builder = (
        SparkSession.builder
        .master(spark.master)
        .appName(spark.app_name)
        .config("spark.jars.packages", packages)
        .config("spark.hadoop.fs.s3a.endpoint", minio.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio.secret_key)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(minio.secure).lower())
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    return builder.getOrCreate()

