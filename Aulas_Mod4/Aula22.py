# Aula 22 - Definição de Schema
# Codito adaptado com insformações aprendidas anteriormente

from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    BooleanType, IntegerType, StringType, StructField, StructType)


# Cria uma instância do SparkSession
spark = SparkSession.builder \
    .appName('fisrtSession') \
    .config('spark.master', 'local[4]') \
    .config('spark.executor.memory', '1gb') \
    .config('spark.shuffle.partitions', 1) \
    .getOrCreate()

# Teste que fiz para ver como tava antes, top
df = spark.read.csv('covid_cases.csv')
df.show(5)
df.describe()


schema = StructType([
    StructField('case_id', IntegerType()),
    StructField('province', StringType()),
    StructField('city', StringType()),
    StructField('group', BooleanType()),
    StructField('infection_case', StringType()),
    StructField('confirmed', IntegerType()),
    StructField('latitude', StringType()),
    StructField('longitude', StringType())
])

caminho = 'covid_cases.csv'

df = spark.read.format('csv').schema(schema).load(caminho)

df.printSchema()
