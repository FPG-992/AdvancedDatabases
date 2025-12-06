import argparse
import time
from pyspark.sql import functions as F
from pyspark.sql import Row
from spark_utils import get_spark

def load_data(spark):
    paths = [
        "/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
        "/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv",
    ]

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(paths)
    )

    df=df.filter(
        (F.col("Vict Age").isNotNull()) & 
        (F.col("Vict Age") > 0) &
        (F.col("Crm Cd Desc") != "") &
        (F.col("Crm Cd Desc").isNotNull()) &
        (F.col("Crm Cd Desc").contains("AGGRAVATED ASSAULT")
        ))
    return df

def create_age_groups_no_udf(df):
    return df.withColumn(
        "age_group",
        F.when(F.col("Vict Age") < 18, "Children")
         .when((F.col("Vict Age") >= 18) & (F.col("Vict Age") <= 24), "Young Adults")
         .when((F.col("Vict Age") >= 25) & (F.col("Vict Age") <= 64), "Adults")
         .otherwise("Elderly")
    )

def create_age_groups_with_udf(df):
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
        
        result_df.groupBy("age_group").count().orderBy(F.desc("count")).show()

    t1 = time.time()
    print(f"Execution time for {args.impl}: {t1 - t0:.5f} seconds")
    spark.stop()
    
if __name__ == "__main__":
    main()