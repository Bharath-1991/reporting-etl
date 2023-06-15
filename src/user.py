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
    # Script generated for node PostgreSQL - moto_dealer_dealeruserprofile
    PostgreSQL_moto_dealer_dealeruserprofile = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_dealeruserprofile",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_dealeruserprofile",
        )
    )

    # Script generated for node PostgreSQL - moto_dealer_motohubaccess
    PostgreSQL_moto_dealer_motohubaccess = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_motohubaccess",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_motohubaccess",
        )
    )

    # Script generated for node PostgreSQL - moto_dealer_user
    PostgreSQL_moto_dealer_user = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_user",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQL_moto_dealer_user",
        )
    )

    return (
        PostgreSQL_moto_dealer_user,
        PostgreSQL_moto_dealer_dealeruserprofile,
        PostgreSQL_moto_dealer_motohubaccess
    )


def transform_data(
        glue_context,
        spark,
        moto_dealer_user,
        moto_dealer_dealeruserprofile,
        moto_dealer_motohubaccess
):
    # Script generated for node Renamed moto_dealer_dealeruserprofile
    Renamed_moto_dealer_dealeruserprofile = ApplyMapping.apply(
        frame=moto_dealer_dealeruserprofile,
        mappings=[
            ("id", "int", "dealeruserprofile_id", "int"),
            ("dealership_id", "int", "dealeruserprofile_dealership_id", "int"),
            ("user_id", "int", "dealeruserprofile_user_id", "int"),
        ],
        transformation_ctx="Renamed_moto_dealer_dealeruserprofile",
    )

    # Script generated for node Renamed moto_dealer_motohubaccess
    Renamed_moto_dealer_motohubaccess = ApplyMapping.apply(
        frame=moto_dealer_motohubaccess,
        mappings=[
            ("id", "int", "motohubaccess_id", "int"),
            ("moto_dealer_user_id", "int", "motohubaccess_moto_dealer_user_id", "int"),
        ],
        transformation_ctx="Renamed_moto_dealer_motohubaccess",
    )

    # Script generated for node Renamed moto_dealer_user
    Renamed_moto_dealer_user = ApplyMapping.apply(
        frame=moto_dealer_user,
        mappings=[
            ("id", "int", "user_id", "int"),
            ("email", "string", "user_email", "string"),
            ("is_active", "boolean", "user_is_active", "boolean"),
            ("source", "string", "user_source", "string"),
        ],
        transformation_ctx="Renamed_moto_dealer_user",
    )

    # Script generated for node Join
    Renamed_moto_dealer_motohubaccessDF = Renamed_moto_dealer_motohubaccess.toDF()
    Renamed_moto_dealer_userDF = Renamed_moto_dealer_user.toDF()
    Join_mdmha_mdu = DynamicFrame.fromDF(
        Renamed_moto_dealer_userDF.join(
            Renamed_moto_dealer_motohubaccessDF,
            (
                Renamed_moto_dealer_userDF["user_id"]
                == Renamed_moto_dealer_motohubaccessDF[
                    "motohubaccess_moto_dealer_user_id"
                ]
            ),
            "left",
        ),
        glue_context,
        "Join_mdmha_mdu",
    )

    # Script generated for node Join
    Join_mdmha_mduDF = Join_mdmha_mdu.toDF()
    Renamed_moto_dealer_dealeruserprofileDF = Renamed_moto_dealer_dealeruserprofile.toDF()
    Join_Join_mdmha_mdu_mddup = DynamicFrame.fromDF(
        Join_mdmha_mduDF.join(
            Renamed_moto_dealer_dealeruserprofileDF,
            (
                Join_mdmha_mduDF["user_id"]
                == Renamed_moto_dealer_dealeruserprofileDF[
                    "dealeruserprofile_user_id"
                ]
            ),
            "left",
        ),
        glue_context,
        "Join_Join_mdmha_mdu_mddup",
    )

    Join_Join_mdmha_mdu_mddup.show(6)

    # Script generated for node SQL Query -dealers
    SQLQuery_dealer = """
    select user_id as dealer_id, user_email as dealer_email, dealeruserprofile_dealership_id as dealer_dealership_id from myDataSource where motohubaccess_id is null and dealeruserprofile_id is not null;
    """
    SQLQuery_node_dealers = sparkSqlQuery(
        glue_context,
        query=SQLQuery_dealer,
        mapping={"myDataSource": Join_Join_mdmha_mdu_mddup},
        transformation_ctx="SQLQuery_node_dealers",
        spark=spark
    )

    # Script generated for node SQL Query - customers
    SQLQuery_customers = """
    select user_id as customer_id, user_email as customer_email, user_source as customer_source from myDataSource where motohubaccess_id is null and dealeruserprofile_id is null;
    """
    SQLQuery_node_customers = sparkSqlQuery(
        glue_context,
        query=SQLQuery_customers,
        mapping={"myDataSource": Join_Join_mdmha_mdu_mddup},
        transformation_ctx="SQLQuery_node_customers",
        spark=spark
    )

    # Script generated for node SQL Query - internalUsers
    SQLQuery_internalUsers = """
    select user_id as internal_user_id, user_email as internal_user_email from myDataSource where motohubaccess_id is not null;
    """
    SQLQuery_node_internalUsers = sparkSqlQuery(
        glue_context,
        query=SQLQuery_internalUsers,
        mapping={"myDataSource": Join_Join_mdmha_mdu_mddup},
        transformation_ctx="SQLQuery_node_internalUsers",
        spark=spark
    )

    return (
        SQLQuery_node_dealers,
        SQLQuery_node_internalUsers,
        SQLQuery_node_customers
    )

def write_data(glue_context, dealers, internalUsers, customers, s3_bucket_path):
    AmazonS3_dealer_purge = glue_context.purge_s3_path(
        f"s3://{s3_bucket_path}/semantic/dealer/",
        options={"retentionPeriod": 1},
        transformation_ctx="AmazonS3_dealer_purge"
    )
    AmazonS3_customer_purge = glue_context.purge_s3_path(
        f"s3://{s3_bucket_path}/semantic/customer/",
        options={"retentionPeriod": 1},
        transformation_ctx="AmazonS3_customer_purge"
    )
    AmazonS3_internal_user_purge = glue_context.purge_s3_path(
        f"s3://{s3_bucket_path}/semantic/internal_user/",
        options={"retentionPeriod": 1},
        transformation_ctx="AmazonS3_internal_user_purge"
    )

    # Script generated for node Amazon S3
    AmazonS3_dealer = glue_context.write_dynamic_frame.from_options(
        frame=dealers,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/dealer/",
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_dealer",
    )

    # Script generated for node Amazon S3
    AmazonS3_customer = glue_context.write_dynamic_frame.from_options(
        frame=customers,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/customer/",
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_customer",
    )

    # Script generated for node Amazon S3
    AmazonS3_internal_user = glue_context.write_dynamic_frame.from_options(
        frame=internalUsers,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/internal_user/",
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_internal_user",
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
    moto_dealer_user, moto_dealer_dealeruserprofile, moto_dealer_motohubaccess = load_data(
        glue_context, db_connection_name
    )
    dealers, internalUsers, customers = transform_data(
        glue_context,
        spark,
        moto_dealer_user,
        moto_dealer_dealeruserprofile,
        moto_dealer_motohubaccess
    )
    write_data(glue_context, dealers, internalUsers, customers, s3_bucket_path)
    job.commit()
