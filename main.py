import sys
from library import DataReader, DataTransformations, UtilityFunctions

if __name__ == "__main__":
    # Validate arguments
    if len(sys.argv) < 2:
        print("Please specify the environment (e.g., dev or prod)")
        sys.exit(-1)
    
    environment = sys.argv[1]

    # Initialize Spark session
    spark = UtilityFunctions.get_spark_session(environment)

    # Read data
    df = DataReader.read_bulk_files(spark, "Data/creditcard.csv")

    df_transformed = DataTransformations.clean_and_transform_data(df)     #data cleaning

    DataReader.save_to_parquet(df_transformed, "data/parquet")   #for dataframes transformations

    DataTransformations.execute_queries(spark, "data/parquet")    # for SQL queries

    print("Application completed!")
