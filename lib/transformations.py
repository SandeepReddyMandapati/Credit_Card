from pyspark.sql.functions import col, log

def clean_and_transform_data(df):
    # Drop nulls
    df_cleaned = df.dropna()
    
    # Normalize and log transformations
    df_transformed = df_cleaned.withColumn("NormalizedAmount", (col("Amount") - df_cleaned.agg({"Amount": "mean"}).first()[0]) / 
                                           df_cleaned.agg({"Amount": "stddev"}).first()[0])
    df_transformed = df_transformed.withColumn("AmountLog", log(col("Amount") + 1))
    return df_transformed

def execute_queries(spark, input_path):
    # Load data
    df = spark.read.parquet(input_path)
    df.createOrReplaceTempView("transactions")

    # Query examples
    avg_normalized_fraud = spark.sql("SELECT AVG(NormalizedAmount) FROM transactions WHERE Class = 1")
    avg_normalized_fraud.show()
