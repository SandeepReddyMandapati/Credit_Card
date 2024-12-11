from pyspark.sql import SparkSession

def get_spark_session(environment: str):
    app_name = f"CreditCardFraudDetection-{environment}"
    return SparkSession.builder.appName(app_name).getOrCreate()
