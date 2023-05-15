# Aula 21 - Consultas por condições
# Codito corrigido com a ajuda do chat GPT

from pyspark.sql import SparkSession
# from pyspark.sql import Row
from pyspark.sql.types import FloatType
import findspark
findspark.init()


# Cria uma instância do SparkSession
spark = SparkSession.builder \
    .master('local') \
    .appName('Aula que Tive que Adivinhar PySpark') \
    .config('spark.executor.memory', '1gb') \
    .getOrCreate()

df = spark.read.csv('Salary_Data.csv', header=False, inferSchema=True)

df = df.withColumnRenamed(
    '_c0', 'AnosExperiencia').withColumnRenamed('_c1', 'Salario')


def converterColuna(dataframe, nomes, novoTipo):
    for nome in nomes:
        # Usa o método withColumn para modificar cada coluna e realizar a conversão de tipo
        dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
    return dataframe


colunas = ['AnosExperiencia', 'Salario']

df = converterColuna(df, colunas, FloatType())


# Exibi apenas as linhas em que o valor da coluna 'Salario' é maior que 5000.
df.filter(df['Salario'] > 5000).show()

# Seleciona apenas a coluna 'Salario', em seguida, filtra onde 'Salario' é maior que 5000 e exibe o resultado.
df.select('Salario').filter(df['Salario'] > 5000).show()

# Exibi apenas onde 'Salario' é maior que 5000 OU o 'AnosExperiencia' é maior que 2.
# O operador | representa a operação lógica OR (OU)
df.filter((df['Salario'] > 5000) | (df['AnosExperiencia'] > 2)).show()

# O operador & representa a operação lógica AND (E)
df.filter((df['Salario'] > 5000) & (df['AnosExperiencia'] > 2)).show()
