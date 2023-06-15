from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from src.trade_in import transform_data
from tests.utils import compare_schema, get_spark


def test_transform_data():
    glueContext, spark = get_spark()
    input_tradein_data = spark.createDataFrame(
        [
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 1, 1, 'test'),
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 2, None, 'pending'),
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 3, 2, 'offered'),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("submit_datetime", TimestampType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("current_offer_id", IntegerType(), True),
            StructField("status", StringType(), True)
        ])
    )

    input_tradeinoffer_data = spark.createDataFrame(
        [
            (1, "pending", datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), None),
            (2, "offered", datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("status", StringType(), True),
            StructField("decision_datetime", TimestampType(), True),
            StructField("offer_date", TimestampType(), True)
        ])
    )

    output_expected = StructType(
        [
            StructField("trade_in_offer_status", StringType(), True),
            StructField("trade_in_decision_date", TimestampType(), True),
            StructField("trade_in_offer_date", TimestampType(), True),
            StructField("trade_in_submitted_date", TimestampType(), True),
            StructField("trade_in_customer_id", IntegerType(), True),
            StructField("trade_in_status", StringType(), True),
        ]
    )

    dynamic_frame_tradein_input = DynamicFrame.fromDF(input_tradein_data, glueContext, "test_tradein")
    dynamic_frame_tradeinoffer_input = DynamicFrame.fromDF(
        input_tradeinoffer_data, glueContext, "test_tradeinoffer"
    )

    output_tradein = transform_data(
        glueContext,
        dynamic_frame_tradein_input,
        dynamic_frame_tradeinoffer_input
    )
    output_tradein_df = output_tradein.toDF()

    assert compare_schema(output_tradein_df.schema, output_expected)
    assert output_tradein_df.count() == 3
