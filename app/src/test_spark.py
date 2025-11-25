from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("advdb-test")
        .config("spark.executor.instances", 4)
        .config("spark.executor.cores", 1)
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )

    print(
        "Executors config:",
        spark.conf.get("spark.executor.instances"),
        spark.conf.get("spark.executor.cores"),
        spark.conf.get("spark.executor.memory"),
    )

    spark.range(10).show()

    spark.stop()


if __name__ == "__main__":
    main()
