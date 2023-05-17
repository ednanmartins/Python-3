# Aula 23 - Introdução

from pyspark.sql.session import SparkSession

from pyspark.sql.types import (BooleanType, IntegerType, StringType,
                               TimestampType, StructType,
                               StructField, ArrayType,
                               TimestampType, FloatType)

import pyspark.sql.functions as F

spark = SparkSession.builder\
    .appName('firstSeesion')\
    .config('spark.master', 'local')\
    .config("spark.executor.memory", "1gb") \
    .config('spark.shuffle.sql.partitions', 1)\
    .getOrCreate()
