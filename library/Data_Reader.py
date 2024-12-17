from pyspark.sql import SparkSession

def read_bulk_files(spark: SparkSession, path: str):
    return spark.read.csv(path, header=True, inferSchema=True)

def save_to_parquet(df, output_path: str):
    df.write.parquet(output_path, mode="overwrite")

