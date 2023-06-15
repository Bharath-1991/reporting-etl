from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

from src.dealership import transform_data
from tests.utils import compare_schema, get_spark


def test_transform_data():
    glueContext, spark = get_spark()
    input_data = spark.createDataFrame(
        [
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "1", "Hyundai Toronto", "Toronto", True, 1, True, "CA", "Hyundai", True, "ON"),
            (2, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "2", "Hyundai Test", "Toronto", True, 1, True, "CA", "Hyundai", True, "ON"),
            (3, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "2", "Hyundai Demo", "Toronto", True, 1, True, "CA", "Hyundai", True, "ON"),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("created", TimestampType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("uid", StringType(), True),
            StructField("name", StringType(), True),
            StructField("city", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("organization_id", IntegerType(), True),
            StructField("compulsory_login", BooleanType(), True),
            StructField("country", StringType(), True),
            StructField("oem", StringType(), True),
            StructField("compulsory_login_vdp", BooleanType(), True),
            StructField("province_code", StringType(), True),
        ])
    )

    output_schema_expected = StructType(
        [
            StructField("dealership_id", IntegerType(), True),
            StructField("dealership_created", TimestampType(), True),
            StructField("dealership_last_updated", TimestampType(), True),
            StructField("dealership_uid", StringType(), True),
            StructField("dealership_name", StringType(), True),
            StructField("dealership_city", StringType(), True),
            StructField("dealership_is_active", BooleanType(), True),
            StructField("dealership_organization_id", IntegerType(), True),
            StructField("dealership_compulsory_login", BooleanType(), True),
            StructField("dealership_country", StringType(), True),
            StructField("dealership_oem", StringType(), True),
            StructField("dealership_compulsory_login_vdp", BooleanType(), True),
            StructField("dealership_province_code", StringType(), True),
            StructField("dealership_is_demo", BooleanType(), True),
        ]
    )

    dynamic_frame_input = DynamicFrame.fromDF(input_data, glueContext, "test_input")

    output = transform_data(glueContext, spark, dynamic_frame_input)
    output_df = output.toDF()
    assert compare_schema(output_df.schema, output_schema_expected)
    demo_dealerships = output.filter(f=lambda x: x["dealership_is_demo"] is True)
    regular_dealerships = output.filter(f=lambda x: x["dealership_is_demo"] is False)
    assert output.count() == 3
    assert demo_dealerships.count() == 2
    assert regular_dealerships.count() == 1
