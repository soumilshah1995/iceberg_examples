from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import sys

# Setup Java and PySpark submit environment
os.environ["JAVA_HOME"] = "/opt/homebrew/Cellar/openjdk@17/17.0.14/libexec/openjdk.jdk/Contents/Home"
SUBMIT_ARGS = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0 "
    "--repositories https://repo1.maven.org/maven2 "
    "pyspark-shell"
)
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ["PYSPARK_PYTHON"] = sys.executable

warehouse_path = "/Users/soumilshah/Desktop/warehouse"

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.dev", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.dev.type", "hadoop")
    .config("spark.sql.catalog.dev.warehouse", warehouse_path)
    .config("spark.sql.catalog.dev.format-version", "3")  # Iceberg V3 for VARIANT
    .config("spark.sql.join.preferSortMergeJoin", "false")
    .getOrCreate()
)

# Create Iceberg table with VARIANT column
spark.sql("""
CREATE TABLE IF NOT EXISTS dev.default.test_table (
    id BIGINT,
    properties VARIANT
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '3'
)
""")


from pyspark.sql.functions import col, parse_json

# Complex JSON to insert
json_string = '''{
    "user": {
        "name": "Alice",
        "age": 30,
        "hobbies": ["reading", "swimming", "hiking"],
        "address": {
            "city": "Wonderland",
            "zip": "12345"
        }
    },
    "status": "active",
    "tags": ["admin", "user", "editor"]
}'''

# Create DataFrame with columns id and JSON string
df = spark.createDataFrame([(1, json_string)], ["id", "json_string"])

# Convert JSON string column to VARIANT type column using parse_json
df_variant = df.withColumn("properties", parse_json(col("json_string"))).drop("json_string")


# Insert into Iceberg table
df_variant.writeTo("dev.default.test_table").append()


spark.sql("""
          SELECT
              id,
              variant_get(properties, '$.user.name', 'string') AS user_name,
              variant_get(properties, '$.user.age', 'int') AS user_age,
              variant_get(properties, '$.user.address.city', 'string') AS city,
              variant_get(properties, '$.status', 'string') AS status
          FROM dev.default.test_table
          """).show(truncate=False)


