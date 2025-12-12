import os
import time
import math
import argparse
from spark_utils import get_spark
from sedona.register import SedonaRegistrator
from pyspark.sql import functions as F


DATA_PATH = os.environ.get("DATA_PATH", "/data")
CRIME_DATA_PATHS = [
    os.path.join(DATA_PATH, "LA_Crime_Data", "LA_Crime_Data_2010_2019.csv"),
    os.path.join(DATA_PATH, "LA_Crime_Data", "LA_Crime_Data_2020_2025.csv"),
]
CENSUS_GEO_PATH = os.path.join(DATA_PATH, "LA_Census_Blocks_2020.geojson")
INCOME_PATH = os.path.join(DATA_PATH, "LA_income_2021.csv")


def load_crimes_2020_2021(spark):
    """
    Loads crime data and filters for the specific analysis window (2020-2021).

    Steps:
    1. Filter out null/zero coordinates.
    2. Parse 'DATE OCC' to create a Timestamp.
    3. Filter for years 2020 and 2021 only.
    4. Convert LAT/LON columns into Sedona Geometry Points.

    Returns:
        DataFrame: Columns ['year', 'geom']
    """
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CRIME_DATA_PATHS)
    )

    df = df.filter(
        F.col("LAT").isNotNull()
        & F.col("LON").isNotNull()
        & (F.col("LAT") != 0.0)
        & (F.col("LON") != 0.0)
        & F.col("DATE OCC").isNotNull()
    )

    # Attempting to read the timestamps with 2 different formats (error safety)
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

    # Create Geometry Point
    df = df.withColumn(
        "geom",
        F.expr("ST_Point(CAST(LON AS DOUBLE), CAST(LAT AS DOUBLE))"),
    )

    return df.select("year", "geom")


def load_blocks_comm_zip(spark):
    """
    Parses the GeoJSON file containing Census Blocks.

    This file contains Polygons representing block boundaries.
    Crucial attributes extracted:
    - COMM: The community name (Neighborhood).
    - ZCTA20: The Zip Code (link to Income Data).
    - POP20: Population count (for weighted averaging).
    - geometry: The Polygon shape itself.

    Returns:
        DataFrame: Columns ['COMM', 'zip_code', 'population', 'geom']
    """
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
    """
    Loads the Income Data CSV.
    
    Cleans the currency column (removes '$' and ',') so it can be cast to Double.

    Returns:
        DataFrame: Columns ['zip_code', 'income_pc']
    """
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


def compute_correlation(df, x_col: str, y_col: str):
    """
    Manually calculates the Pearson Correlation Coefficient (r) between two columns.

    Formula: r = Cov(X, Y) / (StdDev(X) * StdDev(Y))
    
    While Spark has `df.stat.corr()`, calculating the components manually (means,
    sums of squares) gives us more control over null handling and ensures strict
    adherence to the mathematical definition without "hidden" Spark optimizations
    that might skip rows differently.

    Args:
        df (DataFrame): Input data.
        x_col (str): Name of the independent variable column (e.g., income).
        y_col (str): Name of the dependent variable column (e.g., crime rate).

    Returns:
        float or None: The correlation coefficient between -1 and 1, or None if undefined.
    """
    # Aggregate all necessary sums and means in a single pass
    stats = (
        df.select(
            F.count(F.lit(1)).alias("n"),
            F.avg(F.col(x_col)).alias("mean_x"),
            F.avg(F.col(y_col)).alias("mean_y"),
            F.avg(F.col(x_col) * F.col(x_col)).alias("mean_x2"), # Expected value of X^2
            F.avg(F.col(y_col) * F.col(y_col)).alias("mean_y2"), # Expected value of Y^2
            F.avg(F.col(x_col) * F.col(y_col)).alias("mean_xy"), # Expected value of X*Y
        )
        .collect()[0]
    )

    # Error safety
    n = stats["n"]
    if n is None or n <= 1:
        return None
    
    mean_x, mean_y = stats["mean_x"], stats["mean_y"]
    mean_x2, mean_y2 = stats["mean_x2"], stats["mean_y2"]
    mean_xy = stats["mean_xy"]

    if None in (mean_x, mean_y, mean_x2, mean_y2, mean_xy):
        return None

    # Var(X) = E[X^2] - (E[X])^2
    var_x = mean_x2 - mean_x * mean_x
    var_y = mean_y2 - mean_y * mean_y
    cov_xy = mean_xy - mean_x * mean_y

    if var_x <= 0 or var_y <= 0:
        return None

    # Final Calculation
    denom = math.sqrt(var_x * var_y)
    if denom == 0:
        return None

    return cov_xy / denom


def compute_q5_metrics(spark):
    """
    Main Logic for Query 5.

    Workflow:
    1. Spatial Join: Join Crimes (Points) with Census Blocks (Polygons) using `ST_Contains`.
       This assigns every crime to a specific neighborhood (COMM).
    2. Aggregation: Count total crimes per Community for 2020-2021.
    3. Weighted Income: Join Census Blocks with Income Data (via Zip Code).
       Since a Community has multiple blocks/zips, we calculate the Population-Weighted Mean Income.
       Formula: sum(pop * income) / sum(pop)
    4. Rate Calculation: Calculate Annual Crime Rate per Capita.
       Rate = Total Crimes / (Population * 2 Years)
    5. Correlation: Compute Pearson Correlation for:
       - All areas.
       - Top 10 Richest areas.
       - Bottom 10 Poorest areas.
    """
    crimes = load_crimes_2020_2021(spark)
    blocks = load_blocks_comm_zip(spark)
    income = load_income_2021_by_zip(spark)

    # Find which Block Polygon contains each Crime Point
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

    # Count crimes per community
    crime_counts_comm = (
        crimes_with_comm.groupBy("COMM")
        .agg(F.count(F.lit(1)).alias("crimes_2020_2021"))
    )

    # Join Blocks with Income based on Zip Code
    blocks_with_income = blocks.join(income, on="zip_code", how="inner")

    # Aggregate by Community using Weighted Average
    demo_comm = (
        blocks_with_income.groupBy("COMM")
        .agg(
            F.sum("population").alias("population"),
            # Total Income of all people / Total People
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

    # Rate = Crimes / (Population * Years)
    per_comm = per_comm.withColumn(
        "annual_crime_rate",
        F.col("crimes_2020_2021")
        / (F.col("population") * F.lit(2.0)),
    )

    per_comm = per_comm.filter(
        (F.col("population") > 0) & F.col("income_pc").isNotNull()
    )

    # A. Global Correlation
    corr_all = compute_correlation(
        per_comm, "income_pc", "annual_crime_rate"
    )
    print(f"Correlation (all areas): {corr_all}")

    # B. Top 10 Richest
    richest10 = per_comm.orderBy(F.desc("income_pc")).limit(10)
    corr_rich = compute_correlation(
        richest10, "income_pc", "annual_crime_rate"
    )
    print(f"Correlation (top 10 richest areas):   {corr_rich}")

    # C. Bottom 10 Poorest
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
    spark.conf.set("sedona.global.srid", "4326") # how to interpret the coordinate numbers

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
