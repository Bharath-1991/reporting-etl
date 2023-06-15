from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, ShortType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

from src.credit_application import transform_data
from tests.utils import compare_schema, get_spark


def test_transform_data():
    glueContext, spark = get_spark()
    input_data = spark.createDataFrame(
        [
            (1, 2, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 1),
            (2, 1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), None),
            (3, 3, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 2)
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("personal_application_status", ShortType(), True),
            StructField("personal_application_decision_time", TimestampType(), True),
            StructField("personal_application_submit_time", TimestampType(), True),
            StructField("personal_application_id", IntegerType(), True)
        ])
    )

    output_schema_expected = StructType([
        StructField("creditapp_order_id", IntegerType(), True),
        StructField("creditapp_application_id", IntegerType(), True),
        StructField("creditapp_decision_time", TimestampType(), True),
        StructField("creditapp_submit_time", TimestampType(), True),
        StructField("creditapp_status", ShortType(), True)
    ])

    dynamic_frame_input = DynamicFrame.fromDF(input_data, glueContext, "test_input")

    output = transform_data(dynamic_frame_input)
    output_df = output.toDF()
    assert compare_schema(output_df.schema, output_schema_expected)
    assert output.count() == 2
