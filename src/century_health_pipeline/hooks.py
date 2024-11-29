from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
"""
class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
       

        # Load the spark configuration in spark.yaml using the config loader
        parameters = context.config_loader["spark"]
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(context.project_path.name)
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")
"""
class SparkSessionHook:
    @hook_impl
    def after_context_created(self, context) -> None:

        parameters = context.config_loader["spark"]
        spark_conf = SparkConf().setAll(parameters.items())

        project_root = os.path.dirname(os.path.abspath(__file__))
        jar_path = os.path.join(project_root, "jars", "spark-excel_2.12-3.5.1_0.20.4.jar")
        jar_uri = "file:///" + jar_path.replace("\\", "/")

        spark = SparkSession.builder \
            .appName("Kedro Project") \
            .config("spark.jars", jar_uri) \
            .config(conf=spark_conf) \
            .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
            .config("spark.sql.parquet.writeLegacyFormat", "true") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()

        print("Spark Jars Configured:", spark.sparkContext.getConf().get("spark.jars"))
