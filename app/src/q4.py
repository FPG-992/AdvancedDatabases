import os
import time
import argparse
from spark_utils import get_spark
from sedona.register import SedonaRegistrator
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


DATA_PATH = os.environ.get("DATA_PATH", "/data")
CRIME_DATA_PATHS = [
    os.path.join(DATA_PATH, "LA_Crime_Data", "LA_Crime_Data_2010_2019.csv"),
    os.path.join(DATA_PATH, "LA_Crime_Data", "LA_Crime_Data_2020_2025.csv"),
]
POLICE_STATIONS_PATH = os.path.join(DATA_PATH, "LA_Police_Stations.csv")


def load_crimes(spark: SparkSession):
    """
    Loads and preprocesses the Crime Data for geospatial analysis.
    
    Steps:
    1. Loads CSV data.
    2. Casts Latitude/Longitude to Double (required for geometry).
    3. FILTER: Removes rows with NULL coordinates or 'Null Island' (0.0, 0.0).
    4. Generates a unique 'crime_id' for tracking.
    5. Creates a Geometry column 'crime_geom' using Sedona's ST_Point.
    
    Args:
        spark (SparkSession): Active Spark session.
    
    Returns:
        DataFrame: Contains 'crime_id' and 'crime_geom' (Point).
    """
    crimes = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CRIME_DATA_PATHS)
    )

    crimes = (
        crimes
        .withColumn("LAT", F.col("LAT").cast("double"))
        .withColumn("LON", F.col("LON").cast("double"))
        .filter(F.col("LAT").isNotNull() & F.col("LON").isNotNull())
        .filter((F.col("LAT") != 0.0) | (F.col("LON") != 0.0))
        .withColumn("crime_id", F.monotonically_increasing_id())
        .withColumn("crime_geom", F.expr("ST_Point(LON, LAT)"))
    )

    return crimes.select("crime_id", "crime_geom")


def load_police_stations(spark: SparkSession):
    """
    Loads and preprocesses the Police Station locations.

    Steps:
    1. Loads CSV data.
    2. Casts X (Lon) and Y (Lat) to Double.
    3. Creates a Geometry column 'station_geom'.

    Returns:
        DataFrame: Contains 'DIVISION' (Name) and 'station_geom' (Point).
    """
    stations = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(POLICE_STATIONS_PATH)
    )

    stations = (
        stations
        .withColumn("X", F.col("X").cast("double"))
        .withColumn("Y", F.col("Y").cast("double"))
        .withColumn("station_geom", F.expr("ST_Point(X, Y)"))
    )

    return stations.select("DIVISION", "station_geom")


def run_q4(spark: SparkSession):
    """
    Executes the Nearest Neighbor Logic (Query 4).

    Logic:
    1. Cross Join: Matches EVERY crime with EVERY police station.
       - Why? To find the "nearest", we must calculate the distance to all options first.
       - Optimization: We BROADCAST the stations because there are only ~21 of them.
    2. Distance Calc: Uses ST_DistanceSphere (Geodetic) to get meters.
    3. Ranking: Uses Window function to rank stations by distance for each crime.
    4. Aggregation: Groups by the winning station to calculate counts and avg distance.
    """
    crimes = load_crimes(spark)
    stations = load_police_stations(spark)

    # Using Broadcast hash join for better perfromance
    stations_b = F.broadcast(stations)

    joined = (
        crimes
        .crossJoin(stations_b)
        # Calculates distance in METERS between two points (assume Earth curvature)
        .withColumn("distance_m", F.expr("ST_DistanceSphere(crime_geom, station_geom)"))
        .withColumn("distance_km", F.col("distance_m") / 1000.0)
    )

    print("=== Q4: Physical plan (crime-station join with ST_DistanceSphere) ===")
    joined.explain(mode="extended")

    # For each unique crime (partitionBy crime_id), sort the stations by distance
    w = Window.partitionBy("crime_id").orderBy(F.col("distance_m").asc())

    nearest = (
        joined
        # Assign Rank 1 to the closest station, Rank 2 to the second closest, etc.
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1) # Keep only the closest station
    )

    result = (
        nearest
        .groupBy("DIVISION")
        .agg(
            F.avg("distance_km").alias("average_distance_km"),
            F.count("*").alias("count"),
        )
        .orderBy(F.desc("count"))
    )

    print("=== Q4: Results (DIVISION, average_distance_km, count) ===")
    result.show(truncate=False)


def main():
    parser = argparse.ArgumentParser(description="Query 5")
    parser.add_argument("--instances", type=int, default=2)
    parser.add_argument("--cores", type=int, default=1)
    parser.add_argument("--memory", type=str, default="2g")

    args = parser.parse_args()

    # Initializes Spark
    spark = get_spark(
        app_name="advdb-q4-sedona",
        instances=args.instances,
        cores=args.cores,
        memory=args.memory,
    )

    # Registers Sedona SQL functions so Spark recognizes them.
    SedonaRegistrator.registerAll(spark)

    start = time.time()
    run_q4(spark)
    end = time.time()

    # Use consistent wording so run_all.sh summary (grep "Execution time") picks it up.
    print(
        f"Execution time (q4 realistic distances): instances={args.instances}, "
        f"cores={args.cores}, memory={args.memory}: {end - start:.2f} seconds"
    )

    spark.stop()


if __name__ == "__main__":
    main()
