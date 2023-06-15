from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

from src.accessory import transform_data
from tests.utils import compare_schema, get_spark


def test_transform_data():
    glueContext, spark = get_spark()
    input_order_accessories_data = spark.createDataFrame(
        [
            (1, True, 1, 1),
            (1, True, 2, 2),
            (2, True, 1, 3),
        ],
        StructType([
            StructField("order_id", IntegerType(), True),
            StructField("is_installed", BooleanType(), True),
            StructField("accessory_id", IntegerType(), True),
            StructField("id", IntegerType(), True)
        ])
    )

    input_accessories_data = spark.createDataFrame(
        [
            (1, 'Acc 1', '123', 'Acc Desc 1'),
            (2, 'Acc 2', '456', 'Acc Desc 2'),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("title_en", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("description_en", StringType(), True)
        ])
    )

    output_accessory_schema_expected = StructType([
        StructField("accessory_order_id", IntegerType(), True),
        StructField("accessory_is_installed", BooleanType(), True),
        StructField("accessory_name", StringType(), True),
        StructField("accessory_sku", StringType(), True),
        StructField("accessory_description", StringType(), True)
    ])

    output_accessory_aggregated_schema_expected = StructType([
        StructField("agg_accessory_order_id", IntegerType(), True),
        StructField("agg_accessory_count", LongType(), True),
        StructField("agg_accessory_name", StringType(), True),
        StructField("agg_accessory_code", StringType(), True),
        StructField("agg_accessory_description", StringType(), True)
    ])

    dynamic_frame_input_order_accessories = DynamicFrame.fromDF(
        input_order_accessories_data, glueContext, "test_order_accessories"
    )
    dynamic_frame_input_accessories = DynamicFrame.fromDF(
        input_accessories_data, glueContext, "test_accessories"
    )

    output_acc, output_agg_acc = transform_data(
        glueContext, spark, dynamic_frame_input_accessories, dynamic_frame_input_order_accessories
    )
    output_df_accessories = output_acc.toDF()
    output_df_agg_accessories = output_agg_acc.toDF()
    assert compare_schema(output_df_accessories.schema, output_accessory_schema_expected)
    assert compare_schema(
        output_df_agg_accessories.schema, output_accessory_aggregated_schema_expected
    )
    assert output_df_accessories.count() == 3
    assert output_df_agg_accessories.count() == 2
