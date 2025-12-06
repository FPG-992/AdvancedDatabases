import argparse
import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_utils import get_spark

CRIME_DATA_PATH = [
    "/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
    "/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv",
]
RE_CODES_PATH = "/data/RE_codes.csv"

def load_data(spark):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CRIME_DATA_PATH)
    )

    df = df.filter(
        F.col("Vict Descent").isNotNull()
        & (F.col("Vict Descent") != "")
        & F.col("DATE OCC").isNotNull()
        & (F.col("DATE OCC") != "")
    )

    df = df.withColumn(
        "year",
        F.year(F.to_timestamp("DATE OCC", "yyyy MMM dd hh:mm:ss a"))
    )

    df = df.filter(F.col("year").isNotNull())

    df = df.select(
        F.col("year").cast("int"),
        F.col("Vict Descent").alias("vict_descent")
    )

    return df

def load_re_codes(spark):
    re_codes_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(RE_CODES_PATH)
    )

    cols = re_codes_df.columns
    if len(cols) < 2 :
        raise ValueError("RE_codes.csv must have at least two columns(Codes and Description)")
    
    re_codes_df = re_codes_df.select(
        F.col(cols[0]).alias("vict_descent"),
        F.col(cols[1]).alias("vict_desc")
    )

    return re_codes_df

def q2_with_dataframe_api(spark):
    crimes = load_data(spark)
    re_codes = load_re_codes(spark) 

    #1 we count victims per year and descent
    agg = (
        crimes
        .groupBy("year", "vict_descent")
        .agg(F.count(F.lit(1)).alias("cnt"))
    )

    #total victims per year
    total = (
        agg
        .groupBy("year")
        .agg(F.sum("cnt").alias("total_cnt"))
    )

    #calculating percentage
    percentage = (
        agg.join(total, on="year")
        .withColumn("percentage", F.col("cnt") * 100.0 / F.col("total_cnt"))
    )


    #ranking per year (top 3 groups)
    window = Window.partitionBy("year").orderBy(F.desc("cnt"))

    ranked = (
        percentage
        .withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= 3)
    )

    # join with re_codes to get description for gender descent
    result = (
        ranked
        .join(re_codes, on="vict_descent", how="left")
        .select(
            "year",
            F.coalesce(F.col("vict_desc"), F.col("vict_descent")).alias("Victim Descent"),
            F.col("cnt").alias("#"),
            F.round("percentage", 1).alias("%")
        )
        .orderBy(F.desc("year"), F.desc("#"))
    )
    
    result.show(200, truncate=False)

def q2_with_sql_api(spark):
    crimes = load_data(spark)
    re_codes = load_re_codes(spark)

    crimes.createOrReplaceTempView("crimes")
    re_codes.createOrReplaceTempView("re_codes")

    query = """
    WITH agg AS (
        SELECT
            year,
            vict_descent,
            COUNT(*) AS cnt
        FROM crimes
        GROUP BY year, vict_descent
    ),
    total AS (
        SELECT
            year,
            SUM(cnt) AS total
        FROM agg
        GROUP BY year
    ),
    pct AS (
        SELECT
            a.year,
            a.vict_descent,
            a.cnt,
            a.cnt * 100.0 / t.total AS percentage
        FROM agg a
        JOIN total t
          ON a.year = t.year
    ),
    ranked AS (
        SELECT
            year,
            vict_descent,
            cnt,
            percentage,
            RANK() OVER (PARTITION BY year ORDER BY cnt DESC) AS rank
        FROM pct
    )
    SELECT
        r.year,
        COALESCE(rc.vict_desc, r.vict_descent) AS `Victim Descent`,
        r.cnt AS `#`,
        ROUND(r.percentage, 1) AS `%`
    FROM ranked r
    LEFT JOIN re_codes rc
      ON r.vict_descent = rc.vict_descent
    WHERE r.rank <= 3
    ORDER BY r.year DESC, `#` DESC
    """

    result = spark.sql(query)
    result.show(200, truncate=False)

def main():
    parser = argparse.ArgumentParser(description="Query 2")
    parser.add_argument(
        "--impl",
        type=str,
        choices=["dataframe", "sql"],
        required=True,
        help="Implementation to run: dataframe or sql",
    )
    args = parser.parse_args()

    spark = get_spark(
        app_name=f"advdb-q2-{args.impl}",
        instances=4,
        cores=1,
        memory="2g",
    )

    t0 = time.time()
    if args.impl == "dataframe":
        q2_with_dataframe_api(spark)
    else:
        q2_with_sql_api(spark)
    t1 = time.time()

    print(f"Execution time for {args.impl}: {t1 - t0:.5f} seconds")
    spark.stop()

if __name__ == "__main__":
    main()
