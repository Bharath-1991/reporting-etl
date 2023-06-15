from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, ShortType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

from src.vehicle import transform_data
from tests.utils import compare_schema, get_spark


def test_transform_data():
    glueContext, spark = get_spark()
    input_data_inventory = spark.createDataFrame(
        [
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "1", 2022, "Hyundai", "Tuscon", "ix", "black", "black", "auto", "new", "I", "JT1", "1", "gas", "", "", "" ,"" ,True, "Hyundai"),
            (2, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "2", 2023, "Hyundai", "Pallsaide", "ix", "black", "black", "auto", "new", "I", "JT2", "1", "electric", "", "", "" ,"" ,True, "Hyundai"),
            (3, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "3", 2021, "Ford", "Edge", "ix", "black", "black", "auto", "used", "I", "JT3", "1", "gas", "", "", "" ,"" ,True, "Ford"),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("created", TimestampType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("uid", StringType(), True),
            StructField("year", ShortType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("trim", StringType(), True),
            StructField("exterior_color", StringType(), True),
            StructField("interior_color", StringType(), True),
            StructField("transmission", StringType(), True),
            StructField("vehicle_condition", StringType(), True),
            StructField("stock_status", StringType(), True),
            StructField("jato_id", StringType(), True),
            StructField("dealership_uid", StringType(), True),
            StructField("fuel_type", StringType(), True),
            StructField("option_codes", StringType(), True),
            StructField("option_descriptions", StringType(), True),
            StructField("package_codes", StringType(), True),
            StructField("package_descriptions", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("vehicle_oem_id", StringType(), True),
        ])
    )

    input_data_vehicle_option = spark.createDataFrame(
        [
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "OPT1", "Test Option", 1),
            (2, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "OPT1", "Test Option", 2),
            (3, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "OPT2", "Test Option 2", 3),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("created", TimestampType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("option_code", StringType(), True),
            StructField("description", StringType(), True),
            StructField("vehicle_id", IntegerType(), True),
        ])
    )

    input_data_buildstate = spark.createDataFrame(
        [
            (datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "", 1),
            (datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "", 2),
            (datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "", 3),
        ],
        StructType([
            StructField("created", TimestampType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("state", StringType(), True),
            StructField("vehicle_id", IntegerType(), True),
        ])
    )

    input_data_order = spark.createDataFrame(
        [
            (1, 1),
            (2, 2),
            (3, 3),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("vehicle_id", IntegerType(), True),
        ])
    )

    input_data_vehicle = spark.createDataFrame(
        [
            (1, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "JT1"),
            (2, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "JT2"),
            (3, datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), "JT3"),
        ],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("created", TimestampType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("unhaggle_trim_id", StringType(), True),
        ])
    )

    output_schema_expected = StructType(
        [
            StructField("vehicle_id", IntegerType(), True),
            StructField("vehicle_created", TimestampType(), True),
            StructField("vehicle_last_updated", TimestampType(), True),
            StructField("vehicle_uid", StringType(), True),
            StructField("vehicle_year", ShortType(), True),
            StructField("vehicle_make", StringType(), True),
            StructField("vehicle_model", StringType(), True),
            StructField("vehicle_trim", StringType(), True),
            StructField("vehicle_exterior_color", StringType(), True),
            StructField("vehicle_interior_color", StringType(), True),
            StructField("vehicle_transmission", StringType(), True),
            StructField("vehicle_condition", StringType(), True),
            StructField("vehicle_stock_status", StringType(), True),
            StructField("vehicle_jato_id", StringType(), True),
            StructField("vehicle_dealership_uid", StringType(), True),
            StructField("vehicle_fuel_type", StringType(), True),
            StructField("vehicle_option_count", IntegerType(), True),
            StructField("vehicle_option_descriptions", StringType(), True),
            StructField("vehicle_package_count", IntegerType(), True),
            StructField("vehicle_package_descriptions", StringType(), True),
            StructField("vehicle_is_active", BooleanType(), True),
            StructField("vehicle_oem_id", StringType(), True),
        ]
    )

    dynamic_frame_input_inventory = DynamicFrame.fromDF(
        input_data_inventory, glueContext, "test_input_inventory"
    )
    dynamic_frame_input_vehicle_option = DynamicFrame.fromDF(
        input_data_vehicle_option, glueContext, "test_input_vehicle_option")

    dynamic_frame_input_buildstate = DynamicFrame.fromDF(
        input_data_buildstate, glueContext, "test_input_buildstate"
    )
    dynamic_frame_input_order = DynamicFrame.fromDF(
        input_data_order, glueContext, "test_input_order"
    )
    dynamic_frame_input_vehicle = DynamicFrame.fromDF(
        input_data_vehicle, glueContext, "test_input_vehicle"
    )

    output = transform_data(
        glueContext,
        spark,
        dynamic_frame_input_inventory,
        dynamic_frame_input_vehicle_option,
        dynamic_frame_input_buildstate,
        dynamic_frame_input_order,
        dynamic_frame_input_vehicle
    )
    output_df = output.toDF()
    print(output_df.schema)
    print(output_schema_expected)
    assert compare_schema(output_df.schema, output_schema_expected)
    assert output.count() == 3
