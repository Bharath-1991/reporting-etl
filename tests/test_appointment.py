from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, ShortType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

from src.appointment import transform_data
from tests.utils import compare_schema, get_spark


def test_transform_data():
    glueContext, spark = get_spark()
    input_data = spark.createDataFrame(
        [
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 1),
            (2, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 2),
            (3, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 3),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("created", TimestampType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("datetime", TimestampType(), True),
            StructField("order_id", IntegerType(), True)
        ])
    )

    output_schema_expected = StructType([
        StructField("appointment_id", IntegerType(), True),
        StructField("appointment_created", TimestampType(), True),
        StructField("appointment_last_updated", TimestampType(), True),
        StructField("appointment_datetime", TimestampType(), True),
        StructField("appointment_order_id", IntegerType(), True)
    ])

    dynamic_frame_input = DynamicFrame.fromDF(input_data, glueContext, "test_input")

    output = transform_data(dynamic_frame_input)
    output_df = output.toDF()
    assert compare_schema(output_df.schema, output_schema_expected)
    assert output.count() == 3
