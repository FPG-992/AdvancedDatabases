import os
import time
import argparse
from spark_utils import get_spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window


DATA_PATH = os.environ.get("DATA_PATH", "/data")
CRIME_DATA_PATHS = [
    os.path.join(DATA_PATH, "LA_Crime_Data", "LA_Crime_Data_2010_2019.csv"),
    os.path.join(DATA_PATH, "LA_Crime_Data", "LA_Crime_Data_2020_2025.csv"),
]


def load_data(spark):
    """
    Loads the necessary datasets for Query 2.

    1. Crime Data: Loads both the 2010-2019 and 2020-Present datasets.
    2. Race/Ethnicity Codes: Loads the lookup table to resolve Descent codes (e.g., 'H') 
       to full names (e.g., 'Hispanic/Latin/Mexican').

    Args:
        spark (SparkSession): The active Spark session.

    Returns:
        tuple: A tuple containing (crime_df, re_codes_df).
    """
    crime_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CRIME_DATA_PATHS)
    )

    re_codes_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(os.path.join(DATA_PATH, "RE_codes.csv"))
    )

    return crime_df, re_codes_df


def query_dataframe_api(crime_df, re_codes_df):
    """
    Implementation 1: DataFrame API (DSL).

    This function calculates the top 3 victim descent groups per year using 
    Spark's Window functions.
    
    Logic Steps:
    1. Join Crime Data with Race Codes.
    2. Extract the Year from 'DATE OCC'.
    3. Calculate the Total Victims per Year (Window Partitioned by Year).
    4. Rank the Descent groups by count descending (Window Partitioned by Year).
    5. Filter for Rank <= 3.

    Args:
        crime_df (DataFrame): The raw crime data.
        re_codes_df (DataFrame): The race/ethnicity lookup table.

    Returns:
        float: Execution duration in seconds.
    """
    print("--- Running: DataFrame API ---")
    start_time = time.time()

    # Extract Year and Join with Race Codes
    joined_df = (
        crime_df
        .withColumn("year", F.substring(F.col("DATE OCC"), 1, 4))
        .join(re_codes_df, on="Vict Descent", how="inner")
    )

    # Count victims per Year and Descent
    counts_df = joined_df.groupBy("year", "Vict Descent Full").count()

    # Look at ALL rows for a specific year (virtual "bucket") to calculate the total sum
    window_total = Window.partitionBy("year")

    # Look at all rows for a year, ordered by count descending (for ranking)
    window_rank = Window.partitionBy("year").orderBy(F.desc("count"))

    # Apply Windows
    processed_df = (
        counts_df
        .withColumn("total_victims_year", F.sum("count").over(window_total))
        .withColumn("rank", F.row_number().over(window_rank))   # Rank (1st, 2nd, 3rd...)
        .withColumn("percentage", F.format_number((F.col("count") / F.col("total_victims_year")) * 100, 2))
    )

    # Filter for Top 3 and Sort
    result_df = (
        processed_df
        .filter(F.col("rank") <= 3)
        .orderBy(F.desc("year"), F.col("rank"))
        .select(
            F.col("year"),
            F.col("Vict Descent Full").alias("Victim Descent"),
            F.col("count").alias("#"),
            F.col("percentage").alias("%")
        )
    )

    # Trigger Action
    result_df.show(10, truncate=False)
    
    duration = time.time() - start_time

    return duration


def query_sql_api(spark, crime_df, re_codes_df):
    """
    Implementation 2: SQL API.

    This performs the exact same logic as the DataFrame implementation but using 
    Spark SQL queries. This is useful for comparing optimization plans, though 
    Catalyst usually produces identical plans for both APIs.

    Logic Steps:
    1. Registers temporary views.
    2. Uses Common Table Expressions (WITH clauses) to Step-by-Step:
       - Join & Extract Year.
       - Group Counts.
       - Calculate Window Stats (Total & Rank).
       - Select & Format Final Output.

    Args:
        spark (SparkSession): The active Spark session.
        crime_df (DataFrame): The raw crime data.
        re_codes_df (DataFrame): The race/ethnicity lookup table.

    Returns:
        float: Execution duration in seconds.
    """
    print("--- Running: SQL API ---")
    start_time = time.time()

    # Register Temporary Views
    crime_df.createOrReplaceTempView("crime_data")
    re_codes_df.createOrReplaceTempView("re_codes")

    # Execute SQL Query
    query = """
    WITH 
    -- Step 1: Join and Extract Year
    JoinedData AS (
        SELECT 
            substring(c.`DATE OCC`, 1, 4) as year,
            r.`Vict Descent Full` as descent
        FROM crime_data c
        JOIN re_codes r ON c.`Vict Descent` = r.`Vict Descent`
        WHERE c.`Vict Descent` IS NOT NULL
    ),

    -- Step 2: Basic Group By Counts
    GroupedCounts AS (
        SELECT year, descent, COUNT(*) as victims_count
        FROM JoinedData
        GROUP BY year, descent
    ),
    
    -- Step 3: Window Functions (Sum Total & Rank)
    WindowedCalc AS (
        SELECT 
            year, 
            descent, 
            victims_count,
            SUM(victims_count) OVER (PARTITION BY year) as total_year,
            ROW_NUMBER() OVER (PARTITION BY year ORDER BY victims_count DESC) as rn
        FROM GroupedCounts
    )

    -- Step 4: Final Selection and Formatting
    SELECT 
        year, 
        descent as `Victim Descent`, 
        victims_count as `#`, 
        ROUND((victims_count / total_year) * 100, 2) as `%`
    FROM WindowedCalc
    WHERE rn <= 3
    ORDER BY year DESC, `#` DESC
    """

    result_df = spark.sql(query)
    
    # Trigger Action
    result_df.show(10, truncate=False)

    duration = time.time() - start_time

    return duration


def main():
    """
    Main entry point for Query 2 Benchmarking.
    
    1. Parses the '--impl' argument to choose betwen 'df' (DataFrame) or 'sql' (SQL).
    2. Initializes Spark with the assignment-mandated resources:
       - 4 Executors
       - 1 Core per executor
       - 2GB Memory per executor
    3. Runs the chosen implementation and prints the execution time.
    """
    parser = argparse.ArgumentParser(description="Query 2 Benchmark")
    parser.add_argument(
        "--impl",
        choices=["df", "sql"],
        required=True,
        help="Implementation: 'df' or 'sql'"
    )
    args = parser.parse_args()

    spark = get_spark(
        app_name="Query 2",
        instances=4,
        cores=1,
        memory="2g"
    )

    crime_df, re_codes_df = load_data(spark)

    if args.impl == "df":
        duration = query_dataframe_api(crime_df, re_codes_df)
    else:
        duration = query_sql_api(spark, crime_df, re_codes_df)

    print(f"Execution Time ({args.impl}): {duration:.4f} seconds")
    
    spark.stop()


if __name__ == "__main__":
    main()
