import os
import time
import argparse
from spark_utils import get_spark
from pyspark.sql import functions as F
from pyspark.sql import Row


DATA_PATH = './data'
CRIME_DATA_PATHS = [
    os.path.join(DATA_PATH, "LA-Crime", "LA_Crime_Data_2010_2019.csv"),
    os.path.join(DATA_PATH, "LA-Crime", "LA_Crime_Data_2020_2025.csv"),
]


def load_data(spark):
    """
    Loads and pre-filters the crime datasets for Query 1.

    This function reads both the 2010-2019 and 2020-2025 CSV files, infers 
    the schema, and applies the specific filtering criteria required by the assignment:
    1. Valid Victim Age (Not Null and > 0).
    2. Valid Crime Description.
    3. Filters specifically for 'AGGRAVATED ASSAULT' crimes.

    Args:
        spark (SparkSession): The active Spark session.

    Returns:
        DataFrame: A filtered Spark DataFrame containing only the relevant crime records.
    """
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CRIME_DATA_PATHS)
    )

    df = df.filter(
        (F.col("Vict Age").isNotNull()) & 
        (F.col("Vict Age") > 0) &
        (F.col("Crm Cd Desc") != "") &
        (F.col("Crm Cd Desc").isNotNull()) &
        (F.col("Crm Cd Desc").contains("AGGRAVATED ASSAULT")
        ))

    return df


def create_age_groups_no_udf(df):
    """
    Implementation 1: Native DataFrame API (The Optimized Way).

    This function uses standard Spark SQL functions (`F.when`, `F.otherwise`) to 
    categorize victims into age groups. 
    
    Performance Note:
    This is generally the fastest method because Spark's Catalyst Optimizer 
    can compile these operations directly into efficient Java bytecode, avoiding 
    Python-to-JVM serialization overhead.

    Args:
        df (DataFrame): The filtered crime input DataFrame.

    Returns:
        DataFrame: The input DF with a new column 'age_group'.
    """
    return df.withColumn(
        "age_group",
        F.when(F.col("Vict Age") < 18, "Children")
         .when((F.col("Vict Age") >= 18) & (F.col("Vict Age") <= 24), "Young Adults")
         .when((F.col("Vict Age") >= 25) & (F.col("Vict Age") <= 64), "Adults")
         .otherwise("Elderly")
    )


def create_age_groups_with_udf(df):
    """
    Implementation 2: DataFrame with User Defined Function (The Slower Way).

    This function wraps standard Python logic inside a Spark UDF (`F.udf`) 
    to categorize age groups.

    Performance Note:
    This acts as a 'black box' to Spark. To execute this, Spark must serialize 
    every row of data, send it to a Python worker process to run the `age_group` 
    function, and serialize the result back. This serialization cost usually 
    makes it slower than the native API.

    Args:
        df (DataFrame): The filtered crime input DataFrame.

    Returns:
        DataFrame: The input DF with a new column 'age_group' calculated via Python.
    """
    def age_group(age):
        if age < 18:
            return "Children"
        elif 18 <= age <= 24:
            return "Young Adults"
        elif 25 <= age <= 64:
            return "Adults"
        else:
            return "Elderly"

    age_group_udf = F.udf(age_group)

    return df.withColumn("age_group", age_group_udf(F.col("Vict Age")))


def create_age_groups_rdd_api(spark):
    """
    Implementation 3: Low-Level RDD API (The Manual Way).

    This function bypasses the DataFrame API entirely. It converts the data 
    into a Resilient Distributed Dataset (RDD) of Rows and uses functional 
    programming primitives (map, reduceByKey).

    Steps:
    1. Selects 'Vict Age' and converts to RDD.
    2. Maps every row to a tuple: (Age_Group_Name, 1).
    3. Reduces by Key: Sums the '1's for every Age Group.
    4. Sorts the results by count.

    Performance Note:
    This is typically the slowest approach in Python because it cannot use 
    Spark's SQL optimizer and incurs heavy serialization costs for every single record.

    Args:
        spark (SparkSession): The active Spark session.
    """
    df = load_data(spark)
    
    rdd = df.select("Vict Age").rdd
    
    def create_age_group(row: Row):
        age = row["Vict Age"]
        if age < 18:
            age_group = "Children"
        elif 18 <= age <= 24:
            age_group = "Young Adults"
        elif 25 <= age <= 64:
            age_group = "Adults"
        else:
            age_group = "Elderly"
        return (age_group, 1)

    age_group_counts = (
        rdd.map(create_age_group)
        .filter(lambda x: x is not None)
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x : x[1], ascending=False)
    )

    for group, count in age_group_counts.collect():
        print(f"{group}: {count}")


def main():
    """
    Main execution entry point.
    
    1. Parses command line arguments to select the implementation strategy.
    2. Initializes Spark with specific resource limits (4 instances, 1 core, 2GB RAM)
       as required by the assignment benchmarks.
    3. Runs the selected implementation.
    4. Measures and prints the total execution time.
    """
    parser = argparse.ArgumentParser(description = "Query 1")
    
    parser.add_argument(
        "--impl",
        type=str,
        choices=["create_age_groups_no_udf", "create_age_groups_with_udf", "create_age_groups_rdd_api"],
        required=True,
        help="Choose implementation to run",
    )
    args = parser.parse_args()

    spark = get_spark(
        app_name=f"advdb-q1-{args.impl}",
        instances=4,
        cores=1,
        memory="2g",
    )

    t0 = time.time()
    if args.impl == "create_age_groups_rdd_api":
        create_age_groups_rdd_api(spark)
    else:
        df = load_data(spark)

        if args.impl == "create_age_groups_no_udf":
            result_df = create_age_groups_no_udf(df)
        elif args.impl == "create_age_groups_with_udf":
            result_df = create_age_groups_with_udf(df)

        # Trigger Action
        result_df.groupBy("age_group").count().orderBy(F.desc("count")).show()

    t1 = time.time()
    print(f"Execution time for {args.impl}: {t1 - t0:.5f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()
