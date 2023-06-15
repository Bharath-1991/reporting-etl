from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

from src.protection_plan import transform_data
from tests.utils import compare_schema, get_spark


def test_transform_data():
    glueContext, spark = get_spark()
    input_order_pplan_price_data = spark.createDataFrame(
        [
            (1, 1, 1),
            (2, 1, 2),
            (3, 2, 1),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("protectionplanprice_id", IntegerType(), True)
        ])
    )

    input_pplan_price_data = spark.createDataFrame(
        [
            (1, 1),
            (2, 2)
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("protection_plan_id", IntegerType(), True)
        ])
    )

    input_pplan_data = spark.createDataFrame(
        [
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 'PPlan 1', 'PPlan Desc 1', '1234'),
            (2, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 'PPlan 2', 'PPlan Desc 2', '4567'),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("created", TimestampType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("product_code", StringType(), True)
        ])
    )

    output_pplan_schema_expected = StructType([
        StructField("protection_plan_name", StringType(), True),
        StructField("protection_plan_description", StringType(), True),
        StructField("protection_plan_code", StringType(), True),
        StructField("protection_plan_order_id", IntegerType(), True)
    ])

    output_pplan_aggregated_schema_expected = StructType([
        StructField("agg_protection_plan_order_id", IntegerType(), True),
        StructField("agg_protection_plan_count", LongType(), True),
        StructField("agg_protection_plan_name", StringType(), True),
        StructField("agg_protection_plan_code", StringType(), True),
        StructField("agg_protection_plan_description", StringType(), True)
    ])

    dynamic_frame_input_order_pplan_price = DynamicFrame.fromDF(
        input_order_pplan_price_data, glueContext, "test_order_pplans"
    )
    dynamic_frame_input_pplan_price = DynamicFrame.fromDF(
        input_pplan_price_data, glueContext, "test_order_pplans"
    )
    dynamic_frame_input_pplan = DynamicFrame.fromDF(
        input_pplan_data, glueContext, "test_accessories"
    )

    output_pplan, output_agg_pplan = transform_data(
        glueContext,
        spark,
        dynamic_frame_input_pplan,
        dynamic_frame_input_pplan_price,
        dynamic_frame_input_order_pplan_price
    )
    output_df_ppaln = output_pplan.toDF()
    output_df_agg_pplan = output_agg_pplan.toDF()
    assert compare_schema(output_df_ppaln.schema, output_pplan_schema_expected)
    assert compare_schema(
        output_df_agg_pplan.schema, output_pplan_aggregated_schema_expected
    )
    assert output_df_ppaln.count() == 3
    assert output_df_agg_pplan.count() == 2
