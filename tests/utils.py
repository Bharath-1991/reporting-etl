from pyspark.sql.types import StructType
from pyspark import SparkContext
from awsglue.context import GlueContext

def get_spark():
    spark_context = SparkContext.getOrCreate()
    glueContext = GlueContext(spark_context)
    spark = glueContext.spark_session
    return glueContext, spark

def compare_schema(schema_a: StructType, schema_b: StructType) -> bool:
    """
    Utility menthod to comapre two schema and return the results of comparison

    Args:
        schema_a (StructType): Schema for comparison
        schema_b (StructType): Schema for comparison

    Returns:
        bool: Result of schema comparison
    """
    print(len(schema_a))
    print(len(schema_b))
    for a, b in zip(schema_a, schema_b):
        print(a.name, a.dataType)
        print(b.name, b.dataType)
        assert (a.name, a.dataType) == (b.name, b.dataType)
    return len(schema_a) == len(schema_b) and all(
        (a.name, a.dataType) == (b.name, b.dataType)
        for a, b in zip(schema_a, schema_b)
    )