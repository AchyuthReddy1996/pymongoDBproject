import configparser

from pyspark import SparkConf


def get_config(env):
    config = configparser.ConfigParser()
    config.read("conf/pymongodbproject.conf")
    confs = {}
    for key, value in config.items(env):
        confs[key] = value

    return confs


def get_spark_config(env):
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")
    spark_conf = SparkConf()
    for key, value in config.items(env):
        spark_conf.set(key, value)

    return spark_conf
