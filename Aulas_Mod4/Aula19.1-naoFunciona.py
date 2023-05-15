# Aula 19 - Alterando tipo de dados

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import findspark
findspark.init()

# Cria uma instância do SparkSession
spark = SparkSession.builder \
    .master('local') \
    .appName('Aula que Tive que Adivinhar PySpark') \
    .config('spark.executor.memory', '1gb') \
    .getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile('Salary_Data.csv')
rdd = rdd.map(lambda line: line.splir(','))

df = rdd.map(lambda line: Row(AnosExperiencia=line[0], Salario=line[1])).toDF()

# df.printSchema()    # imprime tipo de dado


def converterColuna(dataframe, nomes, novoTipo):
    for nome in nomes:
        dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
    return dataframe


colunas = ['AnosExperiencia', 'Salario']

df = converterColuna(df, colunas, str())

df.show()
df.printSchema()
