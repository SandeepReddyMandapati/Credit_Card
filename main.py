import sys
from library import Data_Reader, transformations, utils

if __name__ == "__main__":
    # Validate arguments
    if len(sys.argv) < 2:
        print("Usage: python main.py <environment> (e.g., dev or prod)")
        sys.exit(-1)

    environment = sys.argv[1]
    print(f"Environment specified: {environment}")

    # Initialize Spark session
    spark = utils.get_spark_session(environment)

    # Read data
    df = Data_Reader.read_bulk_files(spark, "Data/creditcard.csv")

    df_transformed = transformations.clean_and_transform_data(df)     #data cleaning

    Data_Reader.save_to_parquet(df_transformed, "data/parquet")   #for dataframes transformations

    transformations.execute_queries(spark, "data/parquet")    # for SQL queries

    print("Application completed!")
