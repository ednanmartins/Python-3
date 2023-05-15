# Aula 20 - Consultas Simples
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

# df.show()
# df.printSchema()

# Exibe apenas a coluna selecionada
df.select('Salario').show(15)

'''df.groupby('Salario').count()' --> Agrupa os dados por valores únicos da coluna 'Salario'
'sort('Salario', ascendent=False)' --> Ordena o resultado da contagem pela coluna 'Salario' em ordem decrescente
'.show()' --> Exibe o resultado na saída'''
df.groupby('Salario').count().sort('Salario', ascendent=False).show()

# Conta o número de linhas/resultados na coluna selecionada
df.select('Salario').count()

# calcula estatísticas resumidas do DataFrame inteiro
df.describe().show()

# calcula estatísticas resumidas da coluna selecionada
df.describe('Salario').show()

# tras os resultados do DataFrame em formato de Matriz
df.collect()
