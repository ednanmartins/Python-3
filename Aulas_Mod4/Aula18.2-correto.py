# Codito corrigido com a ajuda do chat GPT
from pyspark.sql import SparkSession

# Cria uma instância do SparkSession
spark = SparkSession.builder \
    .master('local') \
    .appName('Aula que Tive que Adivinhar PySpark') \
    .config('spark.executor.memory', '1gb') \
    .getOrCreate()
# Define o modo de execução local
# Define o nome da aplicação
# Configura a memória do executor para 1GB
# Obtém uma sessão Spark existente ou cria uma nova

# Lê o arquivo CSV com opção de inferir o schema e sem header
df = spark.read.csv('Salary_Data.csv', header=False, inferSchema=True)

# o método withColumnRenamed para renomear as colunas _c0 e _c1
# As colunas "_c0" e "_c1" para "AnosExperiencia" e "Salario", respectivamente
df = df.withColumnRenamed(
    '_c0', 'AnosExperiencia').withColumnRenamed('_c1', 'Salario')

# Exibe o DataFrame na saída
df.show()

'''Esse método exibe as primeiras 20 linhas do DataFrame,
juntamente com o esquema das colunas.'''
