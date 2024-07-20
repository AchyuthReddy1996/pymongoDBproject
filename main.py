from lib import configLoader, dataLoader, utils
from pyspark.sql import SparkSession

if __name__ == "__main__":

    print("Loading config file")
    config = configLoader.get_config("MONGODB_DB_CONFS")

    print("Creating spark session")
    print(configLoader.get_config("MONGODB_SPARK_CONFS"))
    spark_session = utils.get_spark_session(appname="mongodb_project")

    print(config)
    """"
        spark_session = SparkSession \
        .builder \
        .master("local") \
        .appName("asm1") \
        .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.3.0') \
        .getOrCreate()
    """


    print("loading data")
    """
    data = (
        spark_session.read.format("mongodb").option("uri", "mongodb://localhost:27017")
            .option("database", "sales")
            .option("collection", "sales")
            .load()
            )
    """

    data = dataLoader.data_load(spark_session, **config)

    print(data.head(5))




