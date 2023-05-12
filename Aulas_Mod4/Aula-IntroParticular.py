# Aula que pesquisei no Youtube pra ter uma introdução previa

'''Importa a classe SparkSession do módulo pyspark.sql.
A SparkSession é a maneira principal de interagir com o Spark
e representa a entrada para a funcionalidade do DataFrame e do SQL.'''
from pyspark.sql import SparkSession

'''Cria uma instância do SparkSession.
O método builder é usado para configurar e inicializar o SparkSession.
master('...') define o modo de execução local para testes e desenvolvimento.
appName('...') define o nome da aplicação Spark.
config('...', '...') configura a memória alocada para execução dos trabalhadores Spark.
getOrCreate() recupera uma SparkSession existente ou cria uma nova se não existir.'''

spark = SparkSession.builder \
    .master('local') \
    .appName('Aula que Tive que Adivinhar PySpark') \
    .config('spark.executor.memory', '1gb') \
    .getOrCreate()


'''Lê um arquivo CSV com nome "Salary_Data.csv" usando o método read.csv do SparkSession.
header=True indica que a primeira linha do arquivo contém os cabeçalhos das colunas.
inferSchema=True permite que o Spark infira automaticamente o tipo de dados de cada coluna.
O DataFrame resultante é atribuído à variável df.
'''
df = spark.read.csv('Salary_Data.csv', header=False, inferSchema=True)

# df.show() exibe as primeiras linhas do DataFrame na saída.
df.show()
