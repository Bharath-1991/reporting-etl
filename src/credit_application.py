import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re


def load_data(glue_context, db_connection_name):
    # Script generated for node PostgreSQL
    PostgreSQL_moto_dealer_order = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.moto_dealer_order",
            "connectionName": db_connection_name,
        },
        transformation_ctx="PostgreSQL_moto_dealer_order",
    )
    return PostgreSQL_moto_dealer_order

def transform_data(moto_dealer_order):
    # Script generated for node Change Schema
    Rename_moto_dealer_order = ApplyMapping.apply(
        frame=moto_dealer_order,
        mappings=[
            ("id", "int", "creditapp_order_id", "int"),
            ("personal_application_status", "short", "creditapp_status", "short"),
            (
                "personal_application_decision_time",
                "timestamp",
                "creditapp_decision_time",
                "timestamp",
            ),
            (
                "personal_application_submit_time",
                "timestamp",
                "creditapp_submit_time",
                "timestamp",
            ),
            ("personal_application_id", "int", "creditapp_application_id", "int"),
        ],
        transformation_ctx="Rename_moto_dealer_order",
    )

    # Script generated for node Filter
    Filter_moto_dealer_order = Filter.apply(
        frame=Rename_moto_dealer_order,
        f=lambda row: (row["creditapp_application_id"] >= 0),
        transformation_ctx="Filter_moto_dealer_order",
    )

    return Filter_moto_dealer_order

def write_data(glue_context, transformed_data, s3_bucket_path):
    AmazonS3_credit_application_purge = glue_context.purge_s3_path(f"s3://{s3_bucket_path}/semantic/credit_application/", options={"retentionPeriod": 1}, transformation_ctx="AmazonS3_credit_application_purge")

    # Script generated for node AWS Glue Data Catalog
    AWSGlueDataCatalog_credit_application = glue_context.write_dynamic_frame.from_options(
        frame=transformed_data,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/credit_application/",
            "partitionKeys": [],
        },
        transformation_ctx="AWSGlueDataCatalog_credit_application",
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
