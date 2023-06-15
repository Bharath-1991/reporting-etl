import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


def sparkSqlQuery(glue_context, query, mapping, transformation_ctx, spark) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glue_context, transformation_ctx)


def write_data(glue_context, transformed_data, s3_bucket_path):
    AmazonS3_dealership_purge = glue_context.purge_s3_path(
        f"s3://{s3_bucket_path}/semantic/dealership/",
        options={"retentionPeriod": 1},
        transformation_ctx="AmazonS3_dealership_purge"
    )

    # Script generated for node Amazon S3
    AmazonS3_dealership = glue_context.write_dynamic_frame.from_options(
        frame=transformed_data,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/dealership/",
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_dealership",
    )


def load_data(glue_context, db_connection_name):
    # Script generated for node PostgreSQL
    PostgreSQL_moto_dealer_dealership = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.moto_dealer_dealership",
            "connectionName": db_connection_name,
        },
        transformation_ctx="PostgreSQL_moto_dealer_dealership",
    )

    return PostgreSQL_moto_dealer_dealership


def transform_data(glue_context, spark, loaded_data):
    # Script generated for node Change Schema
    Rename_moto_dealer_dealership = ApplyMapping.apply(
        frame=loaded_data,
        mappings=[
            ("id", "int", "dealership_id", "int"),
            ("created", "timestamp", "dealership_created", "timestamp"),
            ("last_updated", "timestamp", "dealership_last_updated", "timestamp"),
            ("uid", "string", "dealership_uid", "string"),
            ("name", "string", "dealership_name", "string"),
            ("city", "string", "dealership_city", "string"),
            ("is_active", "boolean", "dealership_is_active", "boolean"),
            ("organization_id", "int", "dealership_organization_id", "int"),
            ("compulsory_login", "boolean", "dealership_compulsory_login", "boolean"),
            ("country", "string", "dealership_country", "string"),
            ("oem", "string", "dealership_oem", "string"),
            (
                "compulsory_login_vdp",
                "boolean",
                "dealership_compulsory_login_vdp",
                "boolean",
            ),
            ("province_code", "string", "dealership_province_code", "string"),
        ],
        transformation_ctx="Rename_moto_dealer_dealership",
    )

    # Script generated for node SQL Query
    SqlQuery56 = """
    select *, 
    CASE WHEN dealership_name RLIKE '(?i).*test.*|.*demo.*' THEN TRUE ELSE FALSE END AS dealership_is_demo
    from myDataSource;
    """
    SQLQuery_add_is_demo = sparkSqlQuery(
        glue_context,
        query=SqlQuery56,
        mapping={"myDataSource": Rename_moto_dealer_dealership},
        transformation_ctx="SQLQuery_add_is_demo",
        spark=spark
    )

    return SQLQuery_add_is_demo


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
    transformed_data = transform_data(glue_context, spark, loaded_data)
    write_data(glue_context, transformed_data, s3_bucket_path)
    job.commit()
