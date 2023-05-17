# Aula 24 - Extract

from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    IntegerType, StringType, StructType, StructField)

# import pyspark.sql.functions as F

# Cria uma sessão Spark
spark = SparkSession.builder\
    .appName('firstSeesion')\
    .config('spark.master', 'local')\
    .config("spark.executor.memory", "1gb") \
    .config('spark.shuffle.sql.partitions', 1)\
    .getOrCreate()

# =====> EXTRACT <=====

# Define o esquema do DataFrame
schema = StructType([StructField('target', StringType()),
                    StructField('_id', IntegerType()),
                    StructField('date', StringType()),
                    StructField('flag', StringType()),
                    StructField('user', StringType()),
                    StructField('text', StringType())
                     ])

# Caminho do arquivo CSV
caminho = 'training.1600000.processed.noemoticon.csv'

# Lê o arquivo CSV com o esquema definido
df = spark.read.format('csv').schema(schema).load(caminho)

# Imprime o esquema do DataFrame
df.printSchema()

# Mostra as primeiras 5 linhas do DataFrame
df.show(5)
