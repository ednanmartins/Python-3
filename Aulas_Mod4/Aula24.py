# Aula 24 - JSON
# Codito adaptado com insformações aprendidas anteriormente

from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    BooleanType, IntegerType, StringType, StructField, StructType, DoubleType)

# Cria uma instância do SparkSession
spark = SparkSession.builder \
    .appName('fisrtSession') \
    .config('spark.master', 'local[4]') \
    .config('spark.executor.memory', '1gb') \
    .config('spark.shuffle.partitions', 1) \
    .getOrCreate()

# Teste que fiz para ver como tava antes, top
df = spark.read.csv('zipcodes.json')
df.show(3)
df.describe()

schema = StructType([
    StructField("RecordNumber", IntegerType()),
    StructField("Zipcode", IntegerType()),
    StructField("ZipCodeType", StringType()),
    StructField("City", StringType()),
    StructField("State", StringType()),
    StructField("LocationType", StringType()),
    StructField("Lat", DoubleType()),
    StructField("Long", DoubleType()),
    StructField("Xaxis", IntegerType()),
    StructField("Yaxis", DoubleType()),
    StructField("Zaxis", DoubleType()),
    StructField("WorldRegion", StringType()),
    StructField("Country", StringType()),
    StructField("LocationText", StringType()),
    StructField("Location", StringType()),
    StructField("Decommisioned", BooleanType()),
    StructField("TaxReturnsFiled", StringType()),
    StructField("EstimatedPopulation", IntegerType()),
    StructField("TotalWages", IntegerType()),
    StructField("Notes", StringType())
])

# Atribuindo o Schema para o DataFrame
df = spark.read.schema(schema).json('zipcodes.json')

# Imprimir Schema
df.printSchema()

# Imprimir o depois
df.show(3)
df.describe()
