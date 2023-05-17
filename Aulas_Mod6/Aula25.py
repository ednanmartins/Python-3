# Aula 25 - Transform

from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    IntegerType, StringType, StructType, StructField, TimestampType)

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


# =====> TRANSFORM <=====

# Remove as colunas 'target' e 'flag'
df = df.drop('target', 'flag')

# Cria novas colunas a partir da coluna 'date'
df = df.withColumn('time', df.date.substr(12, 8))\
    .withColumn('day_week', df.date.substr(1, 3))\
    .withColumn('day', df.date.substr(9, 2))\
    .withColumn('month', df.date.substr(5, 3))\
    .withColumn('year', df.date.substr(25, 4))\
    .drop('date')

# Mostra as duas primeiras linhas do DataFrame após a transformação
df.show(2)


# Função para converter tipos de colunas
def converterColuna(dataframe, nomes, novoTipo):
    for nome in nomes:
        dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
    return dataframe


# Colunas que serão convertidas
colunas_string = ['day_week', 'month']
colunas_inteiro = ['day', 'year']
colunas_time = ['time']

# Converte as colunas para os tipos especificados
df = converterColuna(df, colunas_string, StringType())
df = converterColuna(df, colunas_inteiro, IntegerType())
df = converterColuna(df, colunas_time, TimestampType())

# Imprime o esquema do DataFrame final
df.printSchema()
