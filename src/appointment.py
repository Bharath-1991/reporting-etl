import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def load_data(glue_context, db_connection_name):
    # Script generated for node PostgreSQL - moto_dealer_appointment
    PostgreSQL_moto_dealer_appointment = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_appointment",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_appointment",
        )
    )
    return PostgreSQL_moto_dealer_appointment


def transform_data(moto_dealer_appointment):
    # Script generated for node Change Schema
    Rename_moto_dealer_appointment = ApplyMapping.apply(
        frame=moto_dealer_appointment,
        mappings=[
            ("id", "int", "appointment_id", "int"),
            ("created", "timestamp", "appointment_created", "timestamp"),
            ("last_updated", "timestamp", "appointment_last_updated", "timestamp"),
            ("datetime", "timestamp", "appointment_datetime", "timestamp"),
            ("order_id", "int", "appointment_order_id", "int"),
        ],
        transformation_ctx="Rename_moto_dealer_appointment",
    )

    return Rename_moto_dealer_appointment


def write_data(glue_context, transformed_data, s3_bucket_path):
    AmazonS3_appointment_purge = glue_context.purge_s3_path(
        f"s3://{s3_bucket_path}/semantic/appointment/", options={"retentionPeriod": 1},
        transformation_ctx="AmazonS3_appointment_purge")

    # Script generated for node AWS Glue Data Catalog
    AWSGlueDataCatalog_appointment = glue_context.write_dynamic_frame.from_options(
        frame=transformed_data,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/appointment/",
            "partitionKeys": [],
        },
        transformation_ctx="AWSGlueDataCatalog_appointment",
    )


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_bucket_path", "db_connection_name"])
    s3_bucket_path = args["s3_bucket_path"]
    db_connection_name = args["db_connection_name"]
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    loaded_data = load_data(glue_context, db_connection_name)
    transformed_data = transform_data(loaded_data)
    write_data(glue_context, transformed_data, s3_bucket_path)
    job.commit()
