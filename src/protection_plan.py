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
    # Script generated for node PostgreSQL - order_protection_plan_prices
    PostgreSQL_moto_dealer_order_protection_plan_prices = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_order_protection_plan_prices",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_order_protection_plan_prices",
        )
    )

    # Script generated for node PostgreSQL - protectionplanprice
    PostgreSQL_moto_dealer_protectionplanprice = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_protectionplanprice",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_protectionplanprice",
        )
    )

    # Script generated for node PostgreSQL - protectionplan
    PostgreSQL_moto_dealer_protectionplan = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_protectionplan",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_protectionplan",
        )
    )

    return (
        PostgreSQL_moto_dealer_protectionplan,
        PostgreSQL_moto_dealer_protectionplanprice,
        PostgreSQL_moto_dealer_order_protection_plan_prices
    )


def transform_data(
        glue_context,
        spark,
        moto_dealer_protectionplan,
        moto_dealer_protectionplanprice,
        moto_dealer_order_protection_plan_prices
):
    # Script generated for node Renamed - order_protection_plan_prices
    Rename_moto_dealer_order_protection_plan_prices = ApplyMapping.apply(
        frame=moto_dealer_order_protection_plan_prices,
        mappings=[
            ("id", "int", "mdoppp_id", "int"),
            ("order_id", "int", "mdoppp_order_id", "int"),
            ("protectionplanprice_id", "int", "mdoppp_protectionplanprice_id", "int"),
        ],
        transformation_ctx="Rename_moto_dealer_order_protection_plan_prices",
    )

    # Script generated for node Renamed - protectionplanprice
    Rename_moto_dealer_protectionplanprice = ApplyMapping.apply(
        frame=moto_dealer_protectionplanprice,
        mappings=[
            ("id", "int", "mdppp_id", "string"),
            ("protection_plan_id", "int", "mdppp_protection_plan_id", "int"),
        ],
        transformation_ctx="Rename_moto_dealer_protectionplanprice",
    )

    # Script generated for node Renamed - protectionplan
    Rename_moto_dealer_protectionplan = ApplyMapping.apply(
        frame=moto_dealer_protectionplan,
        mappings=[
            ("id", "int", "mdpp_id", "int"),
            ("created", "timestamp", "mdpp_created", "timestamp"),
            ("last_updated", "timestamp", "mdpp_last_updated", "timestamp"),
            ("name", "string", "mdpp_name", "string"),
            ("description", "string", "mdpp_description", "string"),
            ("product_code", "string", "mdpp_product_code", "string"),
        ],
        transformation_ctx="Rename_moto_dealer_protectionplan",
    )

    # Script generated for node Join
    Join_mdppp_mdpp = Join.apply(
        frame1=Rename_moto_dealer_protectionplanprice,
        frame2=Rename_moto_dealer_protectionplan,
        keys1=["mdppp_protection_plan_id"],
        keys2=["mdpp_id"],
        transformation_ctx="Join_mdppp_mdpp",
    )

    # Script generated for node Join
    Join_mdppp_mdpp_mdoppp = Join.apply(
        frame1=Join_mdppp_mdpp,
        frame2=Rename_moto_dealer_order_protection_plan_prices,
        keys1=["mdppp_id"],
        keys2=["mdoppp_protectionplanprice_id"],
        transformation_ctx="Join_mdppp_mdpp_mdoppp",
    )

    # Script generated for node Rename Feilds
    Rename_Join_mdppp_mdpp_mdoppp = ApplyMapping.apply(
        frame=Join_mdppp_mdpp_mdoppp,
        mappings=[
            ("mdpp_name", "string", "protection_plan_name", "string"),
            ("mdpp_description", "string", "protection_plan_description", "string"),
            ("mdpp_product_code", "string", "protection_plan_code", "string"),
            ("mdoppp_order_id", "int", "protection_plan_order_id", "int"),
        ],
        transformation_ctx="Rename_Join_mdppp_mdpp_mdoppp",
    )

    # Script generated for node SQL Query
    SqlQuery_protection_plan_aggregate = """
    select protection_plan_order_id AS agg_protection_plan_order_id,
    count(protection_plan_code) AS agg_protection_plan_count,
    concat_ws(', ', collect_list(protection_plan_name)) AS agg_protection_plan_name,
    concat_ws(', ', collect_list(protection_plan_code)) AS agg_protection_plan_code,
    concat_ws(', ', collect_list(protection_plan_description)) AS agg_protection_plan_description 
    from myDataSource
    group by protection_plan_order_id;
    """
    SqlQuery_node_protection_plan_aggregate = sparkSqlQuery(
        glue_context,
        query=SqlQuery_protection_plan_aggregate,
        mapping={"myDataSource": Rename_Join_mdppp_mdpp_mdoppp},
        transformation_ctx="SqlQuery_node_protection_plan_aggregate",
        spark=spark
    )

    return Rename_Join_mdppp_mdpp_mdoppp, SqlQuery_node_protection_plan_aggregate


def write_data(glue_context, protection_plans, protection_plans_aggregate, s3_bucket_path):
    AmazonS3_protection_plan_purge = glue_context.purge_s3_path(
        f"s3://{s3_bucket_path}/semantic/protection_plan/", options={"retentionPeriod": 1},
        transformation_ctx="AmazonS3_protection_plan_purge")
    AmazonS3_agg_protection_plan_purge = glue_context.purge_s3_path(
        f"s3://{s3_bucket_path}/semantic/agg_protection_plan/", options={"retentionPeriod": 1},
        transformation_ctx="AmazonS3_agg_protection_plan_purge")

    # Script generated for node Amazon S3
    AmazonS3_protection_plan = glue_context.write_dynamic_frame.from_options(
        frame=protection_plans,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/protection_plan/",
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_protection_plan",
    )

    # Script generated for node Amazon S3
    AmazonS3_agg_protection_plan = glue_context.write_dynamic_frame.from_options(
        frame=protection_plans_aggregate,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/agg_protection_plan/",
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_agg_protection_plan",
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
    (
        moto_dealer_protectionplan,
        moto_dealer_protectionplanprice,
        moto_dealer_order_protection_plan_prices
    ) = load_data(glue_context, db_connection_name)
    protection_plans, protection_plans_aggregate = transform_data(
        glue_context,
        spark,
        moto_dealer_protectionplan,
        moto_dealer_protectionplanprice,
        moto_dealer_order_protection_plan_prices
    )
    write_data(glue_context, protection_plans, protection_plans_aggregate, s3_bucket_path)
    job.commit()
