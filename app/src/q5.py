import argparse
import math
import time
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator
from spark_utils import get_spark

CRIME_DATA_PATH = [
    "/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
    "/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv",
]
CENSUS_GEO_PATH = "/data/LA_Census_Blocks_2020.geojson"
INCOME_PATH = "/data/LA_income_2021.csv"

def compute_correlation(df, x_col: str, y_col: str):
    stats = (
        df.select(
            F.count(F.lit(1)).alias("n"),
            F.avg(F.col(x_col)).alias("mean_x"),
            F.avg(F.col(y_col)).alias("mean_y"),
            F.avg(F.col(x_col) * F.col(x_col)).alias("mean_x2"),
            F.avg(F.col(y_col) * F.col(y_col)).alias("mean_y2"),
            F.avg(F.col(x_col) * F.col(y_col)).alias("mean_xy"),
        )
        .collect()[0]
    )

    n = stats["n"]
    if n is None or n <= 1:
        return None

    mean_x, mean_y = stats["mean_x"], stats["mean_y"]
    mean_x2, mean_y2 = stats["mean_x2"], stats["mean_y2"]
    mean_xy = stats["mean_xy"]

    if None in (mean_x, mean_y, mean_x2, mean_y2, mean_xy):
        return None

    var_x = mean_x2 - mean_x * mean_x
    var_y = mean_y2 - mean_y * mean_y
    cov_xy = mean_xy - mean_x * mean_y

    if var_x <= 0 or var_y <= 0:
        return None

    denom = math.sqrt(var_x * var_y)
    if denom == 0:
        return None

    return cov_xy / denom

def load_crimes_2020_2021(spark):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CRIME_DATA_PATH)
    )

    df = df.filter(
        F.col("LAT").isNotNull()
        & F.col("LON").isNotNull()
        & (F.col("LAT") != 0.0)
        & (F.col("LON") != 0.0)
        & F.col("DATE OCC").isNotNull()
    )

    df = df.withColumn(
        "occ_ts",
        F.coalesce(
            F.to_timestamp(F.col("DATE OCC"), "yyyy MMM dd hh:mm:ss a"),
            F.to_timestamp(F.col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"),
        ),
    ).filter(F.col("occ_ts").isNotNull())

    df = (
        df.withColumn("year", F.year("occ_ts"))
        .filter(F.col("year").between(2020, 2021))
    )

    df = df.withColumn(
        "geom",
        F.expr("ST_Point(CAST(LON AS DOUBLE), CAST(LAT AS DOUBLE))"),
    )

    return df.select("year", "geom")

def load_blocks_comm_zip(spark):
    raw_df = spark.read.option("multiline", "true").json(CENSUS_GEO_PATH)
    
    if "features" in raw_df.columns:
        df = raw_df.select(F.explode(F.col("features")).alias("feature"))
        blocks = df.select(
            F.col("feature.properties.COMM").alias("COMM"),
            F.col("feature.properties.ZCTA20").cast("string").alias("zip_code"),
            F.col("feature.properties.POP20").cast("double").alias("population"),
            F.expr("ST_GeomFromGeoJSON(to_json(feature.geometry))").alias("geom")
        )
    else:
        # Fallback for line-delimited GeoJSON
        blocks = raw_df.select(
            F.col("properties.COMM").alias("COMM"),
            F.col("properties.ZCTA20").cast("string").alias("zip_code"),
            F.col("properties.POP20").cast("double").alias("population"),
            F.expr("ST_GeomFromGeoJSON(to_json(geometry))").alias("geom")
        )

    # Filter invalid
    blocks = blocks.filter(
        F.col("COMM").isNotNull()
        & F.col("zip_code").isNotNull()
        & F.col("population").isNotNull()
        & (F.col("population") > 0)
        & F.col("geom").isNotNull()
    )

    return blocks


def load_income_2021_by_zip(spark):
    income_df = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .csv(INCOME_PATH)
    )

    income_df = income_df.select(
        F.col("Zip Code").cast("string").alias("zip_code"),
        F.regexp_replace(
            F.col("Estimated Median Income"), "[$,]", ""
        ).cast("double").alias("income_pc"),
    )

    income_df = income_df.filter(
        F.col("zip_code").isNotNull() & F.col("income_pc").isNotNull()
    )

    return income_df


def compute_q5_metrics(spark):
    crimes = load_crimes_2020_2021(spark)
    blocks = load_blocks_comm_zip(spark)
    income = load_income_2021_by_zip(spark)

    crimes_with_comm = (
        crimes.alias("c")
        .join(
            blocks.select("COMM", "geom").alias("b"),
            F.expr("ST_Contains(b.geom, c.geom)"),
            how="inner",
        )
        .select(
            F.col("b.COMM").alias("COMM"),
            F.col("c.year").alias("year"),
        )
    )

    crime_counts_comm = (
        crimes_with_comm.groupBy("COMM")
        .agg(F.count(F.lit(1)).alias("crimes_2020_2021"))
    )

    blocks_with_income = blocks.join(income, on="zip_code", how="inner")

    demo_comm = (
        blocks_with_income.groupBy("COMM")
        .agg(
            F.sum("population").alias("population"),
            (
                F.sum(F.col("population") * F.col("income_pc"))
                / F.sum("population")
            ).alias("income_pc"),
        )
    )

    per_comm = (
        demo_comm.join(crime_counts_comm, on="COMM", how="left")
        .fillna({"crimes_2020_2021": 0})
    )

    per_comm = per_comm.withColumn(
        "annual_crime_rate",
        F.col("crimes_2020_2021")
        / (F.col("population") * F.lit(2.0)),
    )

    per_comm = per_comm.filter(
        (F.col("population") > 0) & F.col("income_pc").isNotNull()
    )

    corr_all = compute_correlation(
        per_comm, "income_pc", "annual_crime_rate"
    )
    print(f"Correlation (all areas): {corr_all}")

    richest10 = per_comm.orderBy(F.desc("income_pc")).limit(10)
    corr_rich = compute_correlation(
        richest10, "income_pc", "annual_crime_rate"
    )
    print(f"Correlation (top 10 richest areas):   {corr_rich}")

    poorest10 = per_comm.orderBy(F.col("income_pc").asc()).limit(10)
    corr_poor = compute_correlation(
        poorest10, "income_pc", "annual_crime_rate"
    )
    print(f"Correlation (bottom 10 poorest areas): {corr_poor}")

    print("== Sample per-area statistics (sorted by income) ==")
    per_comm.select(
        "COMM", "population", "income_pc", "annual_crime_rate"
    ).orderBy(F.desc("income_pc")).show(20, truncate=False)

    print("== Physical plan for final aggregation (joins) ==")
    per_comm.explain(mode="formatted")

def main():
    parser = argparse.ArgumentParser(description="Query 5")
    parser.add_argument("--instances", type=int, required=True)
    parser.add_argument("--cores", type=int, required=True)
    parser.add_argument("--memory", type=str, required=True)
    args = parser.parse_args()

    spark = get_spark(
        app_name="advdb-q5-dataframe",
        instances=args.instances,
        cores=args.cores,
        memory=args.memory,
    )

    SedonaRegistrator.registerAll(spark)
    spark.conf.set("sedona.global.srid", "4326")

    t0 = time.time()
    compute_q5_metrics(spark)
    t1 = time.time()

    print(
        f"Execution time for q5 with {args.instances} executors, "
        f"{args.cores} cores, {args.memory} memory: {t1 - t0:.5f} seconds"
    )

    spark.stop()


if __name__ == "__main__":
    main()