from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from awsglue.dynamicframe import DynamicFrame

from src.user import transform_data
from tests.utils import compare_schema, get_spark


def test_transform_data():
    glueContext, spark = get_spark()
    input_user_data = spark.createDataFrame(
        [
            (1, "test@motoinsight.com", True, ""),
            (2, "test@trader.ca", True, ""),
            (3, "test@gmail.com", True, "organic"),
            (4, "test@volvo.ca", True, ""),
            (5, "test@hyundai.com", True, ""),
            (6, "test2@trader.ca", True, "")
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("source", StringType(), True)
        ])
    )

    input_motohubaccess_data = spark.createDataFrame(
        [
            (1, 1),
            (1, 2),
            (1, 6)
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("moto_dealer_user_id", IntegerType(), True),
        ])
    )

    input_dealeruserprofile_data = spark.createDataFrame(
        [
            (1, 1, 1),
            (1, 1, 2),
            (1, 1, 4),
            (1, 2, 5),
            (1, 2, 6)
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("dealership_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
        ])
    )

    output_dealer_schema_expected = StructType(
        [
            StructField("dealer_id", IntegerType(), True),
            StructField("dealer_email", StringType(), True),
            StructField("dealer_dealership_id", IntegerType(), True),
        ]
    )

    output_customer_schema_expected = StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("customer_email", StringType(), True),
            StructField("customer_source", StringType(), True),
        ]
    )

    output_internalUsers_schema_expected = StructType(
        [
            StructField("internal_user_id", IntegerType(), True),
            StructField("internal_user_email", StringType(), True),
        ]
    )

    dynamic_frame_user_input = DynamicFrame.fromDF(input_user_data, glueContext, "test_user")
    dynamic_frame_dealeruserprofile_input = DynamicFrame.fromDF(
        input_dealeruserprofile_data, glueContext, "test_userprofile"
    )
    dynamic_frame_motohubaccess_input = DynamicFrame.fromDF(
        input_motohubaccess_data, glueContext, "test_motohubaccess"
    )

    output_dealer, output_internalUsers, output_customers = transform_data(
        glueContext,
        spark,
        dynamic_frame_user_input,
        dynamic_frame_dealeruserprofile_input,
        dynamic_frame_motohubaccess_input
    )
    output_dealer_df = output_dealer.toDF()
    output_customers_df = output_customers.toDF()
    output_internalUsers_df = output_internalUsers.toDF()

    assert compare_schema(output_dealer_df.schema, output_dealer_schema_expected)
    assert compare_schema(output_customers_df.schema, output_customer_schema_expected)
    assert compare_schema(output_internalUsers_df.schema, output_internalUsers_schema_expected)
    assert output_internalUsers.count() == 3
    assert output_customers.count() == 1
    assert output_dealer.count() == 2
