import argparse
import time

from pyspark.sql import functions as F
from pyspark.sql import Row
from spark_utils import get_spark

CRIME_DATA_PATH = [
    "/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
    "/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv",
]
MO_CODES_PATH = "/data/MO_codes.txt"


def load_crime_mocodes_df(spark):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CRIME_DATA_PATH)
    )

    df = df.filter(
        F.col("Mocodes").isNotNull()
        & (F.col("Mocodes") != "")
    )

    return df.select("Mocodes")


def explode_mocodes_df(crime_df):
    return (
        crime_df
        .select(
            F.explode(F.split(F.col("Mocodes"), " ")).alias("mocode")
        )
        .filter(F.col("mocode") != "")
    )


def load_mo_codes_df(spark):
    raw = spark.read.text(MO_CODES_PATH)

    parsed = raw.select(
        F.regexp_extract("value", r"^(\S+)\s+(.*)$", 1).alias("mocode"),
        F.regexp_extract("value", r"^(\S+)\s+(.*)$", 2).alias("mo_desc"),
    ).filter(
        F.col("mocode").isNotNull() & (F.col("mocode") != "")
    )

    return parsed


def q3_with_dataframe_api(spark, join_hint=None):
    crimes_df = load_crime_mocodes_df(spark)
    exploded = explode_mocodes_df(crimes_df)
    mo_codes_df = load_mo_codes_df(spark)

    if join_hint is not None:
        # broadcast / merge / shuffle_hash / shuffle_replicate_nl
        mo_codes_df = mo_codes_df.hint(join_hint)

    freq = (
        exploded
        .groupBy("mocode")
        .agg(F.count(F.lit(1)).alias("cnt"))
    )

    joined = (
        freq
        .join(mo_codes_df, on="mocode", how="left")
        .select(
            F.col("mocode").alias("MO Code"),
            F.coalesce(F.col("mo_desc"), F.col("mocode")).alias("Description"),
            F.col("cnt").alias("Frequency"),
        )
        .orderBy(F.desc("Frequency"), F.col("MO Code"))
    )

    print("== Physical plan for DataFrame implementation ==")
    joined.explain(mode="formatted")

    joined.show(200, truncate=False)


def load_mo_codes_rdd(spark):
    lines = (
        spark.read.text(MO_CODES_PATH)
        .rdd
        .map(lambda row: row["value"])
        .filter(lambda line: line is not None and line.strip() != "")
    )

    def parse_line(line):
        parts = line.strip().split(None, 1)
        code = parts[0]
        desc = parts[1] if len(parts) > 1 else ""
        return code, desc

    return lines.map(parse_line)


def q3_with_rdd_api(spark):
    crimes_df = load_crime_mocodes_df(spark)
    mocodes_rdd = crimes_df.select("Mocodes").rdd

    def extract_codes(row: Row):
        mocodes_str = row["Mocodes"]
        if not mocodes_str:
            return []
        codes = mocodes_str.split()
        return [(code, 1) for code in codes]

    counts_rdd = (
        mocodes_rdd
        .flatMap(extract_codes)
        .reduceByKey(lambda a, b: a + b)
    )

    mo_codes_rdd = load_mo_codes_rdd(spark)  

    joined_rdd = (
        counts_rdd
        .join(mo_codes_rdd)
        .map(lambda x: (x[0], x[1][0], x[1][1]))
        .sortBy(lambda x: x[1], ascending=False)
    )

    print("Top MO codes (RDD implementation):")
    for code, cnt, desc in joined_rdd.take(200):
        print(f"{code}\t{cnt}\t{desc}")


def main():
    parser = argparse.ArgumentParser(description="Query 3")
    parser.add_argument(
        "--impl",
        type=str,
        choices=["dataframe", "rdd"],
        required=True,
        help="Implementation to run: dataframe or rdd",
    )
    parser.add_argument(
        "--join_hint",
        type=str,
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
    if args.impl == "dataframe":
        q3_with_dataframe_api(spark, join_hint=args.join_hint)
    else:
        q3_with_rdd_api(spark)
    t1 = time.time()

    print(f"Execution time for {args.impl}: {t1 - t0:.5f} seconds")
    spark.stop()


if __name__ == "__main__":
    main()
