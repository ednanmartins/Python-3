# Codito corrigido com a ajuda do chat GPT

from pyspark.sql import SparkSession
# from pyspark.sql import Row
from pyspark.sql.types import StringType
import findspark
findspark.init()


# Cria uma instância do SparkSession
spark = SparkSession.builder \
    .master('local') \
    .appName('Aula que Tive que Adivinhar PySpark') \
    .config('spark.executor.memory', '1gb') \
    .getOrCreate()

# Lê o arquivo CSV com opção de inferir o schema e sem header
df = spark.read.csv('Salary_Data.csv', header=False, inferSchema=True)

# o método withColumnRenamed para renomear as colunas _c0 e _c1
# As colunas "_c0" e "_c1" para "AnosExperiencia" e "Salario", respectivamente
df = df.withColumnRenamed(
    '_c0', 'AnosExperiencia').withColumnRenamed('_c1', 'Salario')

# Exibe o DataFrame na saída
# df.show()
df.printSchema()


# Função para converter as colunas do DataFrame para um novo tipo de dado
def converterColuna(dataframe, nomes, novoTipo):
    for nome in nomes:
        # Usa o método withColumn para modificar cada coluna e realizar a conversão de tipo
        dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
    return dataframe


# Define as colunas alvo que serão convertidas
colunas = ['AnosExperiencia', 'Salario']

# Chama a função para converter as colunas do DataFrame para o tipo StringType
df = converterColuna(df, colunas, StringType())

# Exibe o DataFrame modificado
df.show()

# Exibe o schema atualizado do DataFrame
df.printSchema()
