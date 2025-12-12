import os
import time
import argparse
from spark_utils import get_spark
from pyspark.sql import functions as F
from pyspark.sql import Row


DATA_PATH = os.environ.get("DATA_PATH", "/data")
CRIME_DATA_PATHS = [
    os.path.join(DATA_PATH, "LA_Crime_Data", "LA_Crime_Data_2010_2019.csv"),
    os.path.join(DATA_PATH, "LA_Crime_Data", "LA_Crime_Data_2020_2025.csv"),
]
MO_CODES_PATH = os.path.join(DATA_PATH, "MO_codes.txt")


def load_crime_mocodes_df(spark):
    """
    Loads the crime data and selects strictly the 'Mocodes' column.

    Since Query 3 only concerns the frequency of Modus Operandi (MO) codes,
    loading other columns is unnecessary overhead. We also filter out nulls 
    immediately to prevent errors during the string splitting phase.

    Args:
        spark (SparkSession): The active Spark session.

    Returns:
        DataFrame: A single-column DataFrame containing raw 'Mocodes' strings.
    """
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CRIME_DATA_PATHS)
    )

    df = df.filter(
        F.col("Mocodes").isNotNull()
        & (F.col("Mocodes") != "")
    )

    return df.select("Mocodes")


def load_mo_codes_df(spark):
    """
    Parses the MO_codes.txt file using Regular Expressions.

    Because the file is plain text (not CSV) and relies on the first space 
    acting as the separator between the Code and the Description, we use Regex.

    Regex Logic `^(\S+)\s+(.*)$`:
    - `^(\S+)`: Captures the first word (non-whitespace characters) as the Code.
    - `\s+`: Skips the separator space(s).
    - `(.*)$`: Captures everything else until the end of the line as the Description.

    Returns:
        DataFrame: A structured DF with columns 'mocode' and 'mo_desc'.
    """
    raw = spark.read.text(MO_CODES_PATH)

    parsed = raw.select(
        F.regexp_extract("value", r"^(\S+)\s+(.*)$", 1).alias("mocode"),
        F.regexp_extract("value", r"^(\S+)\s+(.*)$", 2).alias("mo_desc"),
    )

    parsed = parsed.filter(parsed.mocode != "")

    return parsed


def explode_mocodes_df(crime_df):
    """
    Transforms the space-separated MO codes string into individual rows.

    The 'Mocodes' column (of LA Crime Data) contains data like: "0913 1814 2000".
    To count frequencies, we must transform this single row into 3 distinct rows.

    Steps:
    1. regexp_replace: Normalizes whitespace (turns double spaces into single spaces).
    2. split: Converts the string "0913 1814" into an Array ["0913", "1814"].
    3. explode: Transforms the Array into multiple rows (one per code).

    Args:
        crime_df (DataFrame): Input DF with the raw string column.

    Returns:
        DataFrame: A DataFrame where each row represents a single MO Code occurrence.
    """
    # cleaning data
    cleaned = crime_df.select(
        F.regexp_replace(F.col("Mocodes"), r"\s+", " ").alias("mocodes_str")
    )

    exploded = (
        cleaned
        .withColumn("mocode", F.explode(F.split(F.col("mocodes_str"), " ")))
        .filter(F.col("mocode") != "")
        .select("mocode")
    )

    return exploded


def q3_with_dataframe_api(spark, join_hint=None):
    """
    Implementation 1: DataFrame API.

    Orchestrates the loading, cleaning, aggregating, and joining of data.
    Critically, it allows injecting a 'join_hint' to force Spark's Catalyst Optimizer
    to use specific physical join strategies (e.g., Broadcast vs. SortMerge) 
    for benchmarking purposes.

    Args:
        spark (SparkSession): The active session.
        join_hint (str, optional): The strategy name (e.g., 'BROADCAST', 'MERGE').
    """
    crimes_df = load_crime_mocodes_df(spark)
    exploded = explode_mocodes_df(crimes_df)

    mo_codes_df = load_mo_codes_df(spark)

    # Apply hint if provided via CLI
    if join_hint is not None:
        mo_codes_df = mo_codes_df.hint(join_hint)

    # Aggregate counts first (Reduce volume before joining)
    freq = (
        exploded
        .groupBy("mocode")
        .count()
    )
    # names the freq column as count

    # Join with descriptions
    joined = (
        freq
        .join(mo_codes_df, on="mocode", how="left")
        .select(
            F.col("mocode").alias("MO Code"),
            F.coalesce(F.col("mo_desc"), F.col("mocode")).alias("Description"),
            F.col("count").alias("Frequency"),
        )
        .orderBy(F.desc("Frequency"), F.col("MO Code"))
    )
    # coalesce ensures that there are no mocodes without description (if mo_desc is null return mocode) 

    # Print the Physical Plan to confirm which Join Strategy was actually used
    print("== Physical plan for DataFrame implementation ==")
    joined.explain(mode="formatted")

    joined.show(10, truncate=False)


def load_mo_codes_rdd(spark):
    """
    Parses MO Codes using low-level RDD transformations.
    
    Equivalent to `load_mo_codes_df` but uses Python string manipulation
    (`split`, `strip`) applied via `.map()` instead of Spark SQL expressions.
    """
    lines = (
        spark.read.text(MO_CODES_PATH)
        .rdd
        .map(lambda row: row["value"])
        .filter(lambda line: line is not None and line.strip() != "")
    )

    def parse_line(line):
        parts = line.strip().split(None, 1) # None splits by any whitespace, 1: maxsplit
        code = parts[0]
        desc = parts[1] if len(parts) > 1 else ""
        return code, desc

    return lines.map(parse_line)


def q3_with_rdd_api(spark):
    """
    Implementation 2: RDD API.

    Performs the frequency count using the classic MapReduce paradigm:
    1. flatMap: Explodes "A B" into multiple rows "A", "B".
    2. Map: Converts code "A" into tuple ("A", 1).
    3. reduceByKey: Sums the tuples.
    4. Join: Standard RDD join (usually inefficient in Python).

    Performance Note:
    This function is expected to be significantly slower than the DataFrame version
    due to Python serialization overhead and lack of optimizer support.
    """
    crimes_df = load_crime_mocodes_df(spark)
    mocodes_rdd = crimes_df.select("Mocodes").rdd

    def extract_codes(row: Row):
        mocodes_str = row["Mocodes"]
        if not mocodes_str:
            return []
        codes = mocodes_str.split()
        return [(code, 1) for code in codes]

    # Group step: move all (key, value) pairs with the same key to the same machine
    # Reduce: sum them up as they arrive
    counts_rdd = (
        mocodes_rdd
        .flatMap(extract_codes)
        .reduceByKey(lambda a, b: a + b) # a (accumulator), b (next value in the list)
    )

    mo_codes_rdd = load_mo_codes_rdd(spark)

    joined_rdd = (
        counts_rdd
        .join(mo_codes_rdd)  
        .map(lambda x: (x[0], x[1][0], x[1][1])) # x : (code, (freq, description))
        .sortBy(lambda x: x[1], ascending=False)
    )

    print("Top MO codes (RDD implementation):")
    for code, cnt, desc in joined_rdd.take(10):
        print(f"{code}\t{cnt}\t{desc}")


def main():
    """
    Main Entry Point.

    Parses CLI args to determine:
    1. Which implementation to run (DataFrame vs RDD).
    2. (Optional) Which Join Strategy hint to apply to the DataFrame.
    """
    parser = argparse.ArgumentParser(description="Query 3")
    parser.add_argument(
        "--impl",
        choices=["df", "rdd"],
        required=True,
        help="Implementation to run: dataframe or rdd",
    )
    parser.add_argument(
        "--join_hint",
        choices=["broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"],
        default=None,
        help="Optional join strategy hint for DataFrame implementation",
    )
    args = parser.parse_args()

    spark = get_spark(
        app_name=f"advdb-q3-{args.impl}",
        instances=4,
        cores=1,
        memory="2g",
    )

    t0 = time.time()
    if args.impl == "df":
        q3_with_dataframe_api(spark, join_hint=args.join_hint)
    else:
        q3_with_rdd_api(spark)
    t1 = time.time()

    print(f"Execution time for {args.impl}: {t1 - t0:.5f} seconds")
    spark.stop()


if __name__ == "__main__":
    main()
