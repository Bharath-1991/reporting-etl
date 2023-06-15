import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def load_data(glue_context):
    # Script generated for node PostgreSQL - moto_dealer_deposit
    PostgreSQL_moto_dealer_deposit = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_deposit",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_deposit",
        )
    )

    # Script generated for node PostgreSQL - payments_paymenttransaction
    PostgreSQL_payments_paymenttransaction = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.payments_paymenttransaction",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_payments_paymenttransaction",
        )
    )
    return PostgreSQL_moto_dealer_deposit, PostgreSQL_payments_paymenttransaction


def transform_data(moto_dealer_deposit, payments_paymenttransaction):
    # Script generated for node Renamed - moto_dealer_deposit
    Rename_moto_dealer_deposit = ApplyMapping.apply(
        frame=moto_dealer_deposit,
        mappings=[
            ("id", "int", "mdd_id", "int"),
            ("order_id", "int", "mdd_order_id", "int"),
            ("payment_transaction_id", "int", "mdd_payment_transaction_id", "int"),
        ],
        transformation_ctx="Rename_moto_dealer_deposit",
    )

    # Script generated for node Renamed - payments_paymenttransaction
    Rename_payments_paymenttransaction = ApplyMapping.apply(
        frame=payments_paymenttransaction,
        mappings=[
            ("id", "int", "ppt_id", "int"),
            ("created", "timestamp", "ppt_created", "timestamp"),
            ("last_updated", "timestamp", "ppt_last_updated", "timestamp"),
            ("amount", "decimal", "ppt_amount", "decimal"),
            ("currency", "string", "ppt_currency", "string"),
            ("test_payment", "boolean", "ppt_test_payment", "boolean"),
            ("state", "string", "ppt_state", "string"),
            ("amount_refunded", "decimal", "ppt_amount_refunded", "decimal"),
            ("payment_type", "string", "ppt_payment_type", "string"),
            ("refunded_date", "timestamp", "ppt_refunded_date", "timestamp"),
            ("amount_captured", "decimal", "ppt_amount_captured", "decimal"),
        ],
        transformation_ctx="Rename_payments_paymenttransaction",
    )

    # Script generated for node Join
    Join_ppt_mdd = Join.apply(
        frame1=Rename_payments_paymenttransaction,
        frame2=Rename_moto_dealer_deposit,
        keys1=["ppt_id"],
        keys2=["mdd_payment_transaction_id"],
        transformation_ctx="Join_ppt_mdd",
    )

    # Script generated for node Change Schema
    ChangeSchema_Join_ppt_mdd = ApplyMapping.apply(
        frame=Join_ppt_mdd,
        mappings=[
            ("ppt_created", "timestamp", "deposit_date", "timestamp"),
            ("ppt_last_updated", "timestamp", "deposit_last_updated", "timestamp"),
            ("ppt_amount", "decimal", "deposit_amount", "decimal"),
            ("ppt_currency", "string", "deposit_currency", "string"),
            ("ppt_test_payment", "boolean", "deposit_is_test_payment", "boolean"),
            ("ppt_state", "string", "deposit_status", "string"),
            ("ppt_amount_refunded", "decimal", "deposit_refunded_amount", "decimal"),
            ("ppt_payment_type", "string", "deposit_type", "string"),
            ("ppt_refunded_date", "timestamp", "deposit_refunded_date", "timestamp"),
            ("ppt_amount_captured", "decimal", "deposit_amount_captured", "decimal"),
            ("mdd_order_id", "int", "deposit_order_id", "int"),
        ],
        transformation_ctx="ChangeSchema_Join_ppt_mdd",
    )

    return ChangeSchema_Join_ppt_mdd


def write_data(glue_context, deposits):
    AmazonS3_deposit_purge = glue_context.purge_s3_path(f"s3://{s3_bucket_path}/semantic/deposit/",
                                                        options={"retentionPeriod": 1},
                                                        transformation_ctx="AmazonS3_deposit_purge")

    # Script generated for node Amazon S3
    AmazonS3_deposit = glue_context.write_dynamic_frame.from_options(
        frame=deposits,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/deposit/",
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_deposit",
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
    moto_dealer_deposit, payments_paymenttransaction = load_data(glue_context)
    deposits = transform_data(
        moto_dealer_deposit, payments_paymenttransaction
    )
    write_data(glue_context, deposits)
    job.commit()
