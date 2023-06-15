import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glue_context, query, mapping, transformation_ctx, spark) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glue_context, transformation_ctx)


def load_data(glue_context, db_connection_name):
    # Script generated for node PostgreSQL - order_accessories
    PostgreSQL_moto_dealer_order_accessories = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_order_accessories",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_order_accessories",
        )
    )

    # Script generated for node PostgreSQL - accessories
    PostgreSQL_moto_dealer_accessory = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.moto_dealer_accessory",
            "connectionName": db_connection_name,
        },
        transformation_ctx="PostgreSQL_moto_dealer_accessory",
    )

    return PostgreSQL_moto_dealer_accessory, PostgreSQL_moto_dealer_order_accessories


def transform_data(glue_context, spark, moto_dealer_accessory, moto_dealer_order_accessories):
    # Script generated for node Renamed - order_accessories
    Rename_moto_dealer_order_accessories = ApplyMapping.apply(
        frame=moto_dealer_order_accessories,
        mappings=[
            ("order_id", "int", "mdoa_order_id", "int"),
            ("is_installed", "boolean", "mdoa_is_installed", "boolean"),
            ("accessory_id", "int", "mdoa_accessory_id", "int"),
            ("id", "int", "mdoa_id", "int"),
        ],
        transformation_ctx="Rename_moto_dealer_order_accessories",
    )

    # Script generated for node Renamed - accessories
    Rename_moto_dealer_accessory = ApplyMapping.apply(
        frame=moto_dealer_accessory,
        mappings=[
            ("id", "int", "mda_id", "int"),
            ("title_en", "string", "mda_name", "string"),
            ("sku", "string", "mda_sku", "string"),
            ("description_en", "string", "mda_description", "string"),
        ],
        transformation_ctx="Rename_moto_dealer_accessory",
    )

    # Script generated for node Join
    Join_mdoa_mda = Join.apply(
        frame1=Rename_moto_dealer_order_accessories,
        frame2=Rename_moto_dealer_accessory,
        keys1=["mdoa_accessory_id"],
        keys2=["mda_id"],
        transformation_ctx="Join_mdoa_mda",
    )

    # Script generated for node Change Schema
    ChangeSchema_mdoa_mda = ApplyMapping.apply(
        frame=Join_mdoa_mda,
        mappings=[
            ("mdoa_order_id", "int", "accessory_order_id", "int"),
            ("mdoa_is_installed", "boolean", "accessory_is_installed", "boolean"),
            ("mda_name", "string", "accessory_name", "string"),
            ("mda_sku", "string", "accessory_sku", "string"),
            ("mda_description", "string", "accessory_description", "string"),
        ],
        transformation_ctx="ChangeSchema_mdoa_mda",
    )

    # Script generated for node SQL Query
    SqlQuery_accessory_aggregate = """
    select accessory_order_id AS agg_accessory_order_id,
    count(accessory_sku) AS agg_accessory_count,
    concat_ws(', ', collect_list(accessory_name)) AS agg_accessory_name,
    concat_ws(', ', collect_list(accessory_sku)) AS agg_accessory_code,
    concat_ws(', ', collect_list(accessory_description)) AS agg_accessory_description 
    from myDataSource
    group by accessory_order_id;
    """
    SQLQuery_node_accessory_aggregate = sparkSqlQuery(
        glue_context,
        query=SqlQuery_accessory_aggregate,
        mapping={"myDataSource": ChangeSchema_mdoa_mda},
        transformation_ctx="SQLQuery_node_accessory_aggregate",
        spark=spark
    )

    return ChangeSchema_mdoa_mda, SQLQuery_node_accessory_aggregate


def write_data(glue_context, accessories, aggregated_accessories, s3_bucket_path):
    AmazonS3_accessory_purge = glue_context.purge_s3_path(
        f"s3://{s3_bucket_path}/semantic/accessory/", options={"retentionPeriod": 1},
        transformation_ctx="AmazonS3_accessory_purge")
    AmazonS3_agg_accessory_purge = glue_context.purge_s3_path(
        f"s3://{s3_bucket_path}/semantic/agg_accessory/", options={"retentionPeriod": 1},
        transformation_ctx="AmazonS3_agg_accessory_purge")

    # Script generated for node AWS Glue Data Catalog
    AWSGlueDataCatalog_accessory = glue_context.write_dynamic_frame.from_options(
        frame=accessories,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/accessory/",
            "partitionKeys": [],
        },
        transformation_ctx="AWSGlueDataCatalog_accessory",
    )

    # Script generated for node Amazon S3
    AWSGlueDataCatalog_agg_accessory = glue_context.write_dynamic_frame.from_options(
        frame=aggregated_accessories,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/agg_accessory/",
            "partitionKeys": [],
        },
        transformation_ctx="AWSGlueDataCatalog_agg_accessory",
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
    moto_dealer_accessory, moto_dealer_order_accessories = load_data(glue_context, db_connection_name)
    accessories, aggregated_accessories = transform_data(
        glue_context, spark, moto_dealer_accessory, moto_dealer_order_accessories
    )
    write_data(glue_context, accessories, aggregated_accessories, s3_bucket_path)
    job.commit()
