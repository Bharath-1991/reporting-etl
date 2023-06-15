import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

def sparkSqlQuery(glue_context, query, mapping, transformation_ctx, spark) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glue_context, transformation_ctx)

def load_data(glue_context, db_connection_name):
    # Script generated for node PostgreSQL Inventory
    PostgreSQLInventory = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.inventory_inventory",
            "connectionName": db_connection_name,
        },
        transformation_ctx="PostgreSQLInventory",
    )

    # Script generated for node PostgreSQL Vehicle Option
    PostgreSQLVehicleOption = (
        glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": "public.moto_dealer_vehicleoption",
                "connectionName": db_connection_name,
            },
            transformation_ctx="PostgreSQLVehicleOption",
        )
    )

    # Script generated for node PostgreSQL Buildstate
    PostgreSQLBuildstate = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.moto_dealer_buildstate",
            "connectionName": db_connection_name,
        },
        transformation_ctx="PostgreSQLBuildstate",
    )

    # Script generated for node PostgreSQL Order
    PostgreSQLOrder = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.moto_dealer_order",
            "connectionName": db_connection_name,
        },
        transformation_ctx="PostgreSQLOrder",
    )

    # Script generated for node PostgreSQL Vehicle
    PostgreSQLVehicle = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "public.moto_dealer_vehicle",
            "connectionName": db_connection_name,
        },
        transformation_ctx="PostgreSQLVehicle",
    )
    return (
        PostgreSQLInventory,
        PostgreSQLVehicleOption,
        PostgreSQLBuildstate,
        PostgreSQLOrder,
        PostgreSQLVehicle
    )

def transform_data(
        glue_context,
        spark,
        PostgreSQLInventory,
        PostgreSQLVehicleOption,
        PostgreSQLBuildstate,
        PostgreSQLOrder,
        PostgreSQLVehicle
):
    # Script generated for node Change Schema Inventory Join Buildstate
    ChangeSchemaInventoryJoinBuildstate = ApplyMapping.apply(
        frame=PostgreSQLInventory,
        mappings=[
            ("id", "int", "inv_id", "int"),
            ("jato_id", "string", "inv_jato_id", "string"),
            ("vehicle_id", "bigint", "vehicle_id", "long"),
        ],
        transformation_ctx="ChangeSchemaInventoryJoinBuildstate",
    )

    # Script generated for node Change Schema Inventory All
    ChangeSchemaInventoryAll = ApplyMapping.apply(
        frame=PostgreSQLInventory,
        mappings=[
            ("id", "int", "inv_id", "int"),
            ("created", "timestamp", "inv_created", "timestamp"),
            ("last_updated", "timestamp", "inv_last_updated", "timestamp"),
            ("uid", "string", "inv_uid", "string"),
            ("year", "smallint", "inv_year", "short"),
            ("make", "string", "inv_make", "string"),
            ("model", "string", "inv_model", "string"),
            ("trim", "string", "inv_trim", "string"),
            ("exterior_color", "string", "inv_exterior_color", "string"),
            ("interior_color", "string", "inv_interior_color", "string"),
            ("transmission", "string", "inv_transmission", "string"),
            ("vehicle_condition", "string", "inv_vehicle_condition", "string"),
            ("stock_status", "string", "inv_stock_status", "string"),
            ("jato_id", "string", "inv_jato_id", "string"),
            ("dealership_uid", "string", "inv_dealership_uid", "string"),
            ("fuel_type", "string", "inv_fuel_type", "string"),
            ("option_codes", "string", "inv_option_codes", "string"),
            ("option_descriptions", "string", "inv_option_descriptions", "string"),
            ("package_codes", "string", "inv_package_codes", "string"),
            ("package_descriptions", "string", "inv_package_descriptions", "string"),
            ("is_active", "boolean", "inv_is_active", "boolean"),
            ("vehicle_oem_id", "string", "inv_vehicle_oem_id", "string"),
        ],
        transformation_ctx="ChangeSchemaInventoryAll",
    )

    # Script generated for node Change Schema mdvo
    ChangeSchemamdvo = ApplyMapping.apply(
        frame=PostgreSQLVehicleOption,
        mappings=[
            ("id", "int", "mdvo_id", "int"),
            ("created", "timestamp", "mdvo_created", "timestamp"),
            ("last_updated", "timestamp", "mdvo_last_updated", "timestamp"),
            ("option_code", "string", "mdvo_option_code", "string"),
            ("description", "string", "mdvo_description", "string"),
            ("vehicle_id", "int", "mdvo_vehicle_id", "int"),
        ],
        transformation_ctx="ChangeSchemamdvo",
    )

    # Script generated for node Change Schema Buildstate
    ChangeSchemaBuildstate = ApplyMapping.apply(
        frame=PostgreSQLBuildstate,
        mappings=[
            ("created", "timestamp", "buildstate_created", "timestamp"),
            ("last_updated", "timestamp", "buildstate_last_updated", "timestamp"),
            ("state", "string", "buildstate_state", "string"),
            ("order_id", "int", "buildstate_order_id", "int"),
        ],
        transformation_ctx="ChangeSchemaBuildstate",
    )

    # Script generated for node Change Schema Order
    ChangeSchemaOrder = ApplyMapping.apply(
        frame=PostgreSQLOrder,
        mappings=[
            ("id", "int", "order_id", "int"),
            ("vehicle_id", "int", "order_vehicle_id", "int"),
        ],
        transformation_ctx="ChangeSchemaOrder",
    )

    # Script generated for node Change Schema mdv
    ChangeSchemamdv = ApplyMapping.apply(
        frame=PostgreSQLVehicle,
        mappings=[
            ("id", "int", "mdv_id", "int"),
            ("created", "timestamp", "mdv_created", "timestamp"),
            ("last_updated", "timestamp", "mdv_last_updated", "timestamp"),
            ("unhaggle_trim_id", "string", "mdv_unhaggle_trim_id", "string"),
        ],
        transformation_ctx="ChangeSchemamdv",
    )

    # Script generated for node SQL Query Inventory Counts
    SqlQueryInventoryCounts_query = """
    select *,
    CASE inv_option_descriptions WHEN "" THEN 0 WHEN null THEN 0 ELSE size(split(inv_option_descriptions, ',', -1)) END inv_option_count,
    CASE inv_package_descriptions WHEN "" THEN 0 WHEN null THEN 0 ELSE size(split(inv_package_descriptions, ',', -1)) END inv_package_count
    from myDataSource;
    """
    SqlQueryInventoryCounts = sparkSqlQuery(
        glue_context,
        query=SqlQueryInventoryCounts_query,
        mapping={"myDataSource": ChangeSchemaInventoryAll},
        transformation_ctx="SqlQueryInventoryCounts",
        spark=spark
    )

    # Script generated for node Join mvd_mdvo
    Joinmvd_mdvo = Join.apply(
        frame1=ChangeSchemamdv,
        frame2=ChangeSchemamdvo,
        keys1=["mdv_id"],
        keys2=["mdvo_vehicle_id"],
        transformation_ctx="Joinmvd_mdvo",
    )

    # Script generated for node Split Build State
    SqlQuerySplitBuildState_query = """
    select buildstate_created, buildstate_last_updated, buildstate_order_id,
    explode(from_json(buildstate_state, 'ARRAY<STRUCT<`category`: STRUCT<`code`: STRING, `name`: STRING>, `code`: STRING, `custom_filter`: STRING, `is_default`: BOOLEAN, `name`: STRING, `option_code`: STRING, `prices`: STRUCT<`msrp`: BIGINT>, `rule_conflicts`: STRING, `rule_includes`: STRING, `rule_requires`: STRING, `subcategory`: STRING>>'))
    from myDataSource
    where buildstate_order_id is not null 
    and buildstate_state is not null 
    and buildstate_state != '[]';
    """
    SqlQuerySplitBuildState = sparkSqlQuery(
        glue_context,
        query=SqlQuerySplitBuildState_query,
        mapping={"myDataSource": ChangeSchemaBuildstate},
        transformation_ctx="SqlQuerySplitBuildState",
        spark=spark
    )

    # Script generated for node Join Main Vehicle Order
    SqlQueryInventoryCountsDF = (
        SqlQueryInventoryCounts.toDF()
    )
    ChangeSchemaOrderDF = ChangeSchemaOrder.toDF()
    JoinMainVehicleOrder = DynamicFrame.fromDF(
        SqlQueryInventoryCountsDF.join(
            ChangeSchemaOrderDF,
            (
                    SqlQueryInventoryCountsDF["inv_id"]
                    == ChangeSchemaOrderDF["order_vehicle_id"]
            ),
            "left",
        ),
        glue_context,
        "JoinMainVehicleOrder",
    )

    # Script generated for node Build State in Columns
    SqlQueryBuildStateinColumns_query = """
    select buildstate_created,
    buildstate_last_updated,
    buildstate_order_id,
    col.category.code AS buildstate_option_type,
    col.name AS buildstate_option_value,
    col.option_code AS buildstate_option_code
    from myDataSource;
    """
    SqlQueryBuildStateinColumns = sparkSqlQuery(
        glue_context,
        query=SqlQueryBuildStateinColumns_query,
        mapping={"myDataSource": SqlQuerySplitBuildState},
        transformation_ctx="SqlQueryBuildStateinColumns",
        spark=spark
    )

    # Script generated for node Join buildstate order
    Joinbuildstateorder = Join.apply(
        frame1=SqlQueryBuildStateinColumns,
        frame2=ChangeSchemaOrder,
        keys1=["buildstate_order_id"],
        keys2=["order_id"],
        transformation_ctx="Joinbuildstateorder",
    )

    # Script generated for node Join BuildState Inv
    JoinBuildStateInv = Join.apply(
        frame1=Joinbuildstateorder,
        frame2=ChangeSchemaInventoryJoinBuildstate,
        keys1=["order_vehicle_id"],
        keys2=["inv_id"],
        transformation_ctx="JoinBuildStateInv",
    )

    # Script generated for node Join Option Data
    JoinOptionData = Join.apply(
        frame1=JoinBuildStateInv,
        frame2=Joinmvd_mdvo,
        keys1=["inv_jato_id", "buildstate_option_code"],
        keys2=["mdv_unhaggle_trim_id", "mdvo_option_code"],
        transformation_ctx="JoinOptionData",
    )

    # Script generated for node Change Schema Buildstate
    ChangeSchemaBuildstate = ApplyMapping.apply(
        frame=JoinOptionData,
        mappings=[
            ("buildstate_created", "timestamp", "buildstate_created", "timestamp"),
            ("buildstate_option_type", "string", "buildstate_option_type", "string"),
            ("order_id", "int", "order_id", "int"),
            ("mdv_id", "int", "mdv_id", "int"),
            ("mdvo_description", "string", "mdvo_description", "string"),
        ],
        transformation_ctx="ChangeSchemaBuildstate",
    )

    # Script generated for node SQL Query interior color
    SQLQueryinteriorcolor_query = """
    select order_id as int_col_order_id, first(mdvo_description) AS interior_color 
    from myDataSource
    WHERE buildstate_option_type = 'Interior Colors'
    group by order_id, buildstate_created
    order by buildstate_created desc;
    """
    SQLQueryinteriorcolor = sparkSqlQuery(
        glue_context,
        query=SQLQueryinteriorcolor_query,
        mapping={"myDataSource": ChangeSchemaBuildstate},
        transformation_ctx="SQLQueryinteriorcolor",
        spark=spark
    )

    # Script generated for node SQL Query packages
    SQLQuerypackages_query = """
    select order_id as package_order_id, count(mdvo_description) as package_count, concat_ws(',', collect_list(mdvo_description)) AS package_list
    from myDataSource
    WHERE buildstate_option_type = 'Free Standing Packages'
    group by order_id, buildstate_created
    order by buildstate_created desc;
    """
    SQLQuerypackages = sparkSqlQuery(
        glue_context,
        query=SQLQuerypackages_query,
        mapping={"myDataSource": ChangeSchemaBuildstate},
        transformation_ctx="SQLQuerypackages",
        spark=spark
    )

    # Script generated for node SQL Query options
    SQLQueryoptions_query = """
    select order_id as option_order_id, count(mdvo_description) as option_count, concat_ws(',', collect_list(mdvo_description)) AS option_list
    from myDataSource
    WHERE buildstate_option_type = 'Free Standing Options'
    group by order_id, buildstate_created
    order by buildstate_created desc;
    """
    SQLQueryoptions = sparkSqlQuery(
        glue_context,
        query=SQLQueryoptions_query,
        mapping={"myDataSource": ChangeSchemaBuildstate},
        transformation_ctx="SQLQueryoptions",
        spark=spark
    )

    # Script generated for node SQL Query exterior color
    SQLQueryexteriorcolor_query = """
    select order_id as ext_col_order_id, first(mdvo_description) AS exterior_color
    from myDataSource
    WHERE buildstate_option_type = 'Interior Colors'
    group by order_id, buildstate_created
    order by buildstate_created desc;
    """
    SQLQueryexteriorcolor = sparkSqlQuery(
        glue_context,
        query=SQLQueryexteriorcolor_query,
        mapping={"myDataSource": ChangeSchemaBuildstate},
        transformation_ctx="SQLQueryexteriorcolor",
        spark=spark
    )

    # Script generated for node Join color
    SQLQueryinteriorcolorDF = (
        SQLQueryinteriorcolor.toDF()
    )
    SQLQueryexteriorcolorDF = (
        SQLQueryexteriorcolor.toDF()
    )
    Joincolor = DynamicFrame.fromDF(
        SQLQueryinteriorcolorDF.join(
            SQLQueryexteriorcolorDF,
            (
                    SQLQueryinteriorcolorDF["int_col_order_id"]
                    == SQLQueryexteriorcolorDF["ext_col_order_id"]
            ),
            "outer",
        ),
        glue_context,
        "Joincolor",
    )

    # Script generated for node Join pack options
    SQLQueryoptionsDF = SQLQueryoptions.toDF()
    SQLQuerypackagesDF = SQLQuerypackages.toDF()
    Joinpackoptions = DynamicFrame.fromDF(
        SQLQueryoptionsDF.join(
            SQLQuerypackagesDF,
            (
                    SQLQueryoptionsDF["option_order_id"]
                    == SQLQuerypackagesDF["package_order_id"]
            ),
            "outer",
        ),
        glue_context,
        "Joinpackoptions",
    )

    # Script generated for node Join final state
    JoinpackoptionsDF = Joinpackoptions.toDF()
    JoincolorDF = Joincolor.toDF()
    Joinfinalstate = DynamicFrame.fromDF(
        JoinpackoptionsDF.join(
            JoincolorDF,
            (
                    JoinpackoptionsDF["option_order_id"]
                    == JoincolorDF["int_col_order_id"]
            )
            & (
                    JoinpackoptionsDF["option_order_id"]
                    == JoincolorDF["ext_col_order_id"]
            )
            & (
                    JoinpackoptionsDF["package_order_id"]
                    == JoincolorDF["int_col_order_id"]
            )
            & (
                    JoinpackoptionsDF["package_order_id"]
                    == JoincolorDF["ext_col_order_id"]
            ),
            "outer",
        ),
        glue_context,
        "Joinfinalstate",
    )

    # Script generated for node SQL Query Join final build state
    SQLQueryJoinfinalbuildstate_query = """
    select coalesce(option_order_id, package_order_id, int_col_order_id, ext_col_order_id) as order_id,
    interior_color, exterior_color, option_count, option_list, package_count, package_list
    from myDataSource;
    """
    SQLQueryJoinfinalbuildstate = sparkSqlQuery(
        glue_context,
        query=SQLQueryJoinfinalbuildstate_query,
        mapping={"myDataSource": Joinfinalstate},
        transformation_ctx="SQLQueryJoinfinalbuildstate",
        spark=spark
    )

    # Script generated for node Change Schema Final Build state
    ChangeSchemaFinalBuildstate = ApplyMapping.apply(
        frame=SQLQueryJoinfinalbuildstate,
        mappings=[
            ("order_id", "int", "final_bs_order_id", "int"),
            ("option_count", "int", "final_bs_option_count", "int"),
            ("option_list", "string", "final_bs_option_list", "string"),
            ("package_count", "int", "final_bs_package_count", "int"),
            ("package_list", "string", "final_bs_package_list", "string"),
            ("interior_color", "string", "final_bs_interior_color", "string"),
            ("exterior_color", "string", "final_bs_exterior_color", "string"),
        ],
        transformation_ctx="ChangeSchemaFinalBuildstate",
    )

    # Script generated for node Join Main Final
    JoinMainVehicleOrderDF = JoinMainVehicleOrder.toDF()
    ChangeSchemaFinalBuildstateDF = (
        ChangeSchemaFinalBuildstate.toDF()
    )
    JoinMainFinal = DynamicFrame.fromDF(
        JoinMainVehicleOrderDF.join(
            ChangeSchemaFinalBuildstateDF,
            (
                    JoinMainVehicleOrderDF["order_id"]
                    == ChangeSchemaFinalBuildstateDF["final_bs_order_id"]
            ),
            "left",
        ),
        glue_context,
        "JoinMainFinal",
    )

    # Script generated for node Final SQL Transform
    FinalSQLTransform_query = """
    select
    inv_id vehicle_id, 
    inv_created vehicle_created,
    inv_last_updated vehicle_last_updated,
    inv_uid vehicle_uid,
    inv_year vehicle_year,
    inv_make vehicle_make,
    inv_model vehicle_model,
    inv_trim vehicle_trim,
    coalesce(final_bs_exterior_color, inv_exterior_color) vehicle_exterior_color,
    coalesce(final_bs_interior_color, inv_interior_color) vehicle_interior_color,
    inv_transmission vehicle_transmission,
    inv_vehicle_condition vehicle_condition,
    inv_stock_status vehicle_stock_status,
    inv_jato_id vehicle_jato_id,
    inv_dealership_uid vehicle_dealership_uid,
    inv_fuel_type vehicle_fuel_type,
    coalesce(final_bs_option_count, inv_option_count) vehicle_option_count,
    coalesce(final_bs_option_list, inv_option_descriptions) vehicle_option_descriptions,
    coalesce(final_bs_package_count, inv_package_count) vehicle_package_count,
    coalesce(final_bs_package_list, inv_package_descriptions) vehicle_package_descriptions,
    inv_is_active vehicle_is_active,
    inv_vehicle_oem_id vehicle_oem_id
    from myDataSource;
    """
    FinalSQLTransform = sparkSqlQuery(
        glue_context,
        query=FinalSQLTransform_query,
        mapping={"myDataSource": JoinMainFinal},
        transformation_ctx="FinalSQLTransform",
        spark=spark
    )
    return FinalSQLTransform

def write_data(glue_context, transformed_data, s3_bucket_path):
    AmazonS3_vehicle_purge = glue_context.purge_s3_path(f"s3://{s3_bucket_path}/semantic/vehicle/", options={"retentionPeriod": 1}, transformation_ctx="AmazonS3_vehicle_purge")

    # Script generated for node AWS Glue Data Catalog
    AWSGlueDataCatalog_vehicle = glue_context.write_dynamic_frame.from_options(
        frame=transformed_data,
        connection_type="s3",
        format="glueparquet",
        format_options={"compression": "gzip"},
        connection_options={
            "path": f"s3://{s3_bucket_path}/semantic/vehicle/",
            "partitionKeys": [],
        },
        transformation_ctx="AWSGlueDataCatalog_vehicle",
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
    (
        PostgreSQLInventory,
        PostgreSQLVehicleOption,
        PostgreSQLBuildstate,
        PostgreSQLOrder,
        PostgreSQLVehicle
    ) = load_data(glue_context, db_connection_name)
    transformed_data = transform_data(
        glue_context,
        spark,
        PostgreSQLInventory,
        PostgreSQLVehicleOption,
        PostgreSQLBuildstate,
        PostgreSQLOrder,
        PostgreSQLVehicle
    )
    write_data(glue_context, transformed_data, s3_bucket_path)
    job.commit()
