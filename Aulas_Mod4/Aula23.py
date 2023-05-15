# Aula 23 - Operações e consultas com DF
# Codito adaptado com insformações aprendidas anteriormente

from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    BooleanType, IntegerType, StringType, StructField, StructType)
import pyspark.sql.functions as F


# Cria uma instância do SparkSession
spark = SparkSession.builder \
    .appName('fisrtSession') \
    .config('spark.master', 'local[4]') \
    .config('spark.executor.memory', '1gb') \
    .config('spark.shuffle.partitions', 1) \
    .getOrCreate()

# Teste que fiz para ver como tava antes, top
df = spark.read.csv('covid_cases.csv')
df.show(3)
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
df.show(3)
df.describe()

cases = df.withColumnRenamed('infection_case', 'Casos de Infecção')
cases = df.toDF(*['ID', 'Província', 'Cidade', 'Grupo',
                'Casos de Infecção', 'Confirmados', 'Latitude', 'Longitude'])
cases.show(3)

df2 = df.select('province', 'city', 'confirmed')
df2.show(3)

df3 = df.sort(F.desc('confirmed'))
df3.show()
