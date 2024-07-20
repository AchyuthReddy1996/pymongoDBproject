import configparser

from pyspark import SparkConf


def get_config(level):
    config = configparser.ConfigParser()
    config.read("conf/pymongodbproject.conf")
    confs = {}
    for key, value in config.items(level):
        confs[key] = value

    return confs


def get_spark_config():
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")
    spark_conf = SparkConf()
    for key, value in config.items():
        spark_conf.set(key, value)

    return spark_conf
