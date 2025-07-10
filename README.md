#ESSENTIALS

pip install pyspark faker schedule

# Use SparkSession builder with Delta support
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("DeltaPipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
