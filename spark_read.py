from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def demonstrate_parquet_filter_behavior(parquet_path, filter_column, filter_values):
    """
    Demonstrates that applying filters on a Parquet DataFrame in Spark
    does not necessarily trigger a full re-read of the entire dataset for each filter.

    Args:
        parquet_path (str): The path to the Parquet files.
        filter_column (str): The column to filter on.
        filter_values (list): A list of values to filter for.
    """

    spark = SparkSession.builder.appName("ParquetFilterDemo").getOrCreate()

    # Read the Parquet data
    df = spark.read.parquet(parquet_path)

    for value in filter_values:
        print(f"Filtering for {filter_column} = {value}")

        # Apply the filter
        filtered_df = df.filter(col(filter_column) == value)

        # Force evaluation to trigger the query execution.
        # This will show the spark plan and if the full read happens.
        filtered_df.count()

        # Explain the physical plan. This will show if the full table is scanned or only the filtered partitions.
        print("Physical Plan:")
        filtered_df.explain(extended=True)
        print("-" * 40)

    spark.stop()

# Example usage (replace with your actual Parquet path and filter criteria)
parquet_path = "part-00000-0eaeb61a-27ca-4aa0-8b58-d23713f29f0c.c000.snappy.parquet"  # Replace with your Parquet path
filter_column = "state"  # Replace with your filter column
filter_values = ["CA", "NY", "TX"]  # Replace with your filter values

demonstrate_parquet_filter_behavior(parquet_path, filter_column, filter_values)