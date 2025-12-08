import argparse
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from sedona.register import SedonaRegistrator


CRIME_DATA_PATHS = [
    "/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
    "/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv",
]
POLICE_STATIONS_PATH = "/data/LA_Police_Stations.csv"


def create_spark_session(app_name: str, instances: int, cores: int, memory: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.executor.instances", str(instances))
        .config("spark.executor.cores", str(cores))
        .config("spark.executor.memory", memory)
        .config("spark.sql.shuffle.partitions", str(instances * cores * 4))
    )

    spark = builder.getOrCreate()

    SedonaRegistrator.registerAll(spark)

    spark.conf.set("sedona.global.srid", "4326")

    return spark


def load_crimes(spark: SparkSession):
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
    crimes = load_crimes(spark)
    stations = load_police_stations(spark)

    stations_b = F.broadcast(stations)

    joined = (
        crimes
        .crossJoin(stations_b)
        .withColumn("distance_m", F.expr("ST_DistanceSphere(crime_geom, station_geom)"))
        .withColumn("distance_km", F.col("distance_m") / 1000.0)
    )

    print("=== Q4: Physical plan (crime–station join with ST_DistanceSphere) ===")
    joined.explain(mode="extended")

    w = Window.partitionBy("crime_id").orderBy(F.col("distance_m").asc())

    nearest = (
        joined
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--instances", type=int, default=2)
    parser.add_argument("--cores", type=int, default=1)
    parser.add_argument("--memory", type=str, default="2g")
    return parser.parse_args()


def main():
    args = parse_args()

    spark = create_spark_session(
        "advdb-q4-sedona",
        args.instances,
        args.cores,
        args.memory,
    )

    start = time.time()
    run_q4(spark)
    end = time.time()

    print(
        f"Q4 runtime (realistic distances): instances={args.instances}, "
        f"cores={args.cores}, memory={args.memory} -> {end - start:.2f} sec"
    )

    spark.stop()


if __name__ == "__main__":
    main()
