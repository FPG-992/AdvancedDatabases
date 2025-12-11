from pyspark.sql import SparkSession


# The URL of the Spark Master node. 
# Port 7077 is the default port for the Spark Standalone Cluster Manager.
# This tells the driver program where to request resources to run the job.
SPARK_MASTER = "spark://spark-master:7077"


def get_spark(
        app_name: str,
        instances: int = 4,
        cores: int = 1,
        memory: str = "2g"
    ) -> SparkSession:
    """
    Initializes a SparkSession with specific resource constraints for benchmarking.

    This function acts as a wrapper around the SparkSession builder. It is designed 
    to programmatically set the number of executors, cores, and memory allocation. 

    Args:
        app_name (str): The name of the application as it will appear in the Spark UI. 
        instances (int, optional): The number of executors (workers) to request. 
        cores (int, optional): The number of CPU cores to allocate per executor. 
        memory (str, optional): The amount of RAM to allocate per executor (e.g., '2g', '4g'). 

    Returns:
        SparkSession: The entry point to programming Spark with the Dataset and DataFrame API.
    """
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(app_name)
        .config("spark.executor.instances", instances)
        .config("spark.executor.cores", cores)
        .config("spark.executor.memory", memory)
        .config("spark.sql.shuffle.partitions", str(instances * cores * 4))
        .getOrCreate()
    )

    return spark
