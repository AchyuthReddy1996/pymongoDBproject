from pyspark.sql import SparkSession
from lib import configLoader


def get_spark_session(appname):
    builder = SparkSession.builder.master("local[*]").appName(appname)
    conf_dict = configLoader.get_config("MONGODB_SPARK_CONFS")
    for key,value in conf_dict.items():
        builder = builder.config(str(key),value)
    spark_session = builder.getOrCreate()

    return spark_session
