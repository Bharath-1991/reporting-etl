import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


def load_data(glue_context, db_connection_name):
    # Script generated for node PostgreSQL - moto_dealer_tradein
    PostgreSQL_moto_dealer_tradein = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_tradein",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_tradein",
        )
    )

    # Script generated for node PostgreSQL - moto_dealer_tradeinoffer
    PostgreSQL_moto_dealer_tradeinoffer = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_tradeinoffer",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_tradeinoffer",
        )
    )
    return PostgreSQL_moto_dealer_tradein, PostgreSQL_moto_dealer_tradeinoffer

def transform_data(glue_context, moto_dealer_tradein, moto_dealer_tradeinoffer):
    # Script generated for node Renamed - moto_dealer_tradein
    Rename_moto_dealer_tradein = ApplyMapping.apply(
        frame=moto_dealer_tradein,
        mappings=[
            ("id", "int", "mdti_id", "int"),
            ("submit_datetime", "timestamp", "mdti_submit_datetime", "timestamp"),
            ("user_id", "int", "mdti_user_id", "int"),
            ("current_offer_id", "int", "mdti_current_offer_id", "int"),
            ("status", "string", "mdti_status", "string"),
        ],
        transformation_ctx="Rename_moto_dealer_tradein",
    )

    # Script generated for node Renamed - moto_dealer_tradeinoffer
    Rename_moto_dealer_tradeinoffer = ApplyMapping.apply(
        frame=moto_dealer_tradeinoffer,
        mappings=[
            ("id", "int", "mdtio_id", "int"),
            ("status", "string", "mdtio_status", "string"),
            ("decision_datetime", "timestamp", "mdtio_decision_datetime", "timestamp"),
            ("offer_date", "timestamp", "mdtio_offer_date", "timestamp"),
        ],
        transformation_ctx="Rename_moto_dealer_tradeinoffer",
    )

    # Script generated for node Join
    Rename_moto_dealer_tradeinofferDF = (
        Rename_moto_dealer_tradeinoffer.toDF()
    )
    Rename_moto_dealer_tradeinDF = (
        Rename_moto_dealer_tradein.toDF()
    )
    Join_mdto_mdt = DynamicFrame.fromDF(
        Rename_moto_dealer_tradeinofferDF.join(
            Rename_moto_dealer_tradeinDF,
            (
                Rename_moto_dealer_tradeinofferDF["mdtio_id"]
                == Rename_moto_dealer_tradeinDF["mdti_current_offer_id"]
            ),
            "right",
        ),
        glue_context,
        "Join_mdto_mdt",
    )

    # Script generated for node Change Schema
    Rename_SelectFields_Join_mdto_mdt = ApplyMapping.apply(
        frame=Join_mdto_mdt,
        mappings=[
            ("mdtio_status", "string", "trade_in_offer_status", "string"),
            ("mdtio_decision_datetime", "timestamp", "trade_in_decision_date", "timestamp"),
            ("mdtio_offer_date", "timestamp", "trade_in_offer_date", "timestamp"),
            ("mdti_submit_datetime", "timestamp", "trade_in_submitted_date", "timestamp"),
            ("mdti_user_id", "int", "trade_in_customer_id", "int"),
            ("mdti_status", "string", "trade_in_status", "string"),
        ],
        transformation_ctx="Rename_SelectFields_Join_mdto_mdt",
    )

    return Rename_SelectFields_Join_mdto_mdt


def write_data(glue_context, transformed_data, s3_bucket_path):
    AmazonS3_trade_in_purge = glue_context.purge_s3_path(f"s3://{s3_bucket_path}/semantic/trade_in/", options={"retentionPeriod": 1}, transformation_ctx="AmazonS3_trade_in_purge")

    # Script generated for node Amazon S3
    AmazonS3_trade_in = glue_context.write_dynamic_frame.from_options(
        frame=transformed_data,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/trade_in/",
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_trade_in",
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
    trade_in, trade_in_offers = load_data(glue_context, db_connection_name)
    transformed_data = transform_data(glue_context, trade_in, trade_in_offers)
    write_data(glue_context, transformed_data, s3_bucket_path)
    job.commit()
