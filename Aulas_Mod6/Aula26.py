# Aula 26 - Load

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
df = spark.read.csv(caminho, header=False)
df.show(2)

# Lê o arquivo CSV com o esquema definido
df = spark.read.format('csv').schema(schema).load(caminho)

# Imprime o esquema do DataFrame
# df.printSchema()

# Mostra as primeiras 5 linhas do DataFrame
df.show(2)


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
df = converterColuna(df, colunas_time, StringType())

# Imprime o esquema do DataFrame final
# df.printSchema()


# =====> LOAD <=====

# Função que cria BD no Mondo DB
def get_database():
    from pymongo import MongoClient

    # Definindo a string de conexão com o MongoDB
    CONNECTION_STRING = "mongodb+srv://root:5OkOyu53TE2xdC2i@cluster0.tnzynwo.mongodb.net"

    # Criando uma instância do cliente MongoClient com a string de conexão
    client = MongoClient(CONNECTION_STRING)

    # Retornando o banco de dados "etl_soul_on"
    return client['etl_soul_on']


# Obtendo a instância do banco de dados
dbname = get_database()

# Obtendo a coleção "data_load" dentro do banco de dados
collection_name = dbname['data_load']

# Limitando o DataFrame a 20 linhas
df = df.limit(20)

# Convertendo o DataFrame para um DataFrame Pandas
df_pandas = df.toPandas()

# Convertendo o DataFrame Pandas para um dicionário de registros
data_dict = df_pandas.to_dict('records')

# Inserindo os registros na coleção "data_load"
collection_name.insert_many(data_dict)

# Imprimindo uma mensagem de sucesso
print('Data Frame importado com sucesso!')
