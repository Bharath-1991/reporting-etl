from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType, DecimalType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from decimal import Decimal

from src.deposit import transform_data
from tests.utils import compare_schema, get_spark


def test_transform_data():
    glueContext, spark = get_spark()
    input_deposit = spark.createDataFrame(
        [
            (1, 1, 1),
            (2, 2, 2),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("payment_transaction_id", IntegerType(), True)
        ])
    )

    input_paymenttransaction = spark.createDataFrame(
        [
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), Decimal(1000.0), 'USD', False, 'confirmed', Decimal(0.0), 'card', None, Decimal(1000.0)),
            (2, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), Decimal(1000.0), 'USD', False, 'refunded', Decimal(1000.0), 'card', datetime.strptime("2023-01-02 00:00:00", "%Y-%m-%d %H:%M:%S"), Decimal(1000.0)),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("created", TimestampType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("amount", DecimalType(10,2), True),
            StructField("currency", StringType(), True),
            StructField("test_payment", BooleanType(), True),
            StructField("state", StringType(), True),
            StructField("amount_refunded", DecimalType(10,2), True),
            StructField("payment_type", StringType(), True),
            StructField("refunded_date", TimestampType(), True),
            StructField("amount_captured", DecimalType(10,2), True)
        ])
    )

    output_deposit_expected = StructType([
        StructField("deposit_date", TimestampType(), True),
        StructField("deposit_last_updated", TimestampType(), True),
        StructField("deposit_amount", DecimalType(10,2), True),
        StructField("deposit_currency", StringType(), True),
        StructField("deposit_is_test_payment", BooleanType(), True),
        StructField("deposit_status", StringType(), True),
        StructField("deposit_refunded_amount", DecimalType(10,2), True),
        StructField("deposit_type", StringType(), True),
        StructField("deposit_refunded_date", TimestampType(), True),
        StructField("deposit_amount_captured", DecimalType(10,2), True),
        StructField("deposit_order_id", IntegerType(), True)
    ])

    dynamic_frame_input_deposit = DynamicFrame.fromDF(
        input_deposit, glueContext, "test_deposit"
    )
    dynamic_frame_input_paymenttransaction = DynamicFrame.fromDF(
        input_paymenttransaction, glueContext, "test_paymenttransaction"
    )

    output_deposit = transform_data(
        dynamic_frame_input_deposit,
        dynamic_frame_input_paymenttransaction
    )
    output_df_deposit = output_deposit.toDF()
    assert compare_schema(output_df_deposit.schema, output_deposit_expected)
    assert output_df_deposit.count() == 2
