from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType


def sales_schema():

    schema = StructType([
        StructField("ordernumber", IntegerType(), nullable=False),
        StructField("quantityordered", IntegerType()),
        StructField("priceeach", FloatType()),
        StructField("orderlinenumber", IntegerType()),
        StructField("sales", FloatType()),
        StructField("orderdate", StringType()),
        StructField("status", StringType()),
        StructField("qtr_id", IntegerType()),
        StructField("month_id", IntegerType()),
        StructField("year_id", IntegerType()),
        StructField("productline", StringType()),
        StructField("msrp", IntegerType()),
        StructField("productcode", StringType()),
        StructField("customername", StringType()),
        StructField("phone", StringType()),
        StructField("addressline1", StringType()),
        StructField("addressline2", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("postalcode", StringType()),
        StructField("country", StringType()),
        StructField("territory", StringType()),
        StructField("contactlastname", StringType()),
        StructField("contactfirstname", StringType()),
        StructField("dealsize", StringType())
        ]
    )
    return schema


def data_load(spark,**options):
    loader = (spark.read
              .format("mongodb")
              .option("uri",options["uri"])
              .option("database",options["database"])
              .option("collection",options["collection"])
              .load()
              )
    return loader
