from pyspark.sql import SparkSession

SPARK_MASTER = "spark://spark-master:7077"

def get_spark(app_name: str,
              instances: int = 4,
              cores: int = 1,
              memory: str = "2g") -> SparkSession:

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(app_name)
        .config("spark.executor.instances", instances)
        .config("spark.executor.cores", cores)
        .config("spark.executor.memory", memory)
        .getOrCreate()
    )
    return spark