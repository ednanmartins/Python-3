'''DESCRIÇÃO
Os estudantes deverão realizar um processo de ETL – Extract, Transform, Load
usando uma base de dados csv ou json de sua preferência. Após o tratamento dos datasets,
este deverá ser carregado em uma base de dados MongoDB.'''

from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    StringType, StructType, StructField, FloatType, IntegerType)

# ==========>> PROCEDIMENTOS <<==========

# ===> 1-Cada estudante deverá selecionar um dataset, sendo OBRIGATORIAMENTE um em CSV ou JSON no site https://www.kaggle.com/datasets.

# Escolhi: https://www.kaggle.com/datasets/ashpalsingh1525/imdb-movies-dataset?select=imdb_movies.csv

spark = SparkSession.builder\
    .appName('firstSeesion')\
    .config('spark.master', 'local')\
    .config("spark.executor.memory", "1gb") \
    .config('spark.shuffle.sql.partitions', 1)\
    .getOrCreate()

caminho = 'imdb_movies.csv'
# ======================================================================


# ===> 2-O dataset deverá ser carregado em um script em Python usando PySpark.
df = spark.read.csv(caminho, header=True)

# df.printSchema()
df.show(2)
# ======================================================================


# ===> 3-O dataset deve ser tratado usando PySpark e demais tecnologias aprendidas durante o curso.

schema = StructType([StructField('names', StringType()),
                     StructField('date_x', StringType()),
                     StructField('score', FloatType()),
                     StructField('genre', StringType()),
                     StructField('overview', StringType()),
                     StructField('crew', StringType()),
                     StructField('orig_title', StringType()),
                     StructField('status', StringType()),
                     StructField('orig_lang', StringType()),
                     StructField('budget_x', StringType()),
                     StructField('revenue', StringType()),
                     StructField('country', StringType())
                     ])

# Lê o DF e adiciona a opção '.option('header', Thue) para dizer que o DF ja tem Titulos
df_trad = spark.read.format('csv').schema(
    schema).option('header', True).load(caminho)

# Remove as colunas desnecessarias
df_trad = df_trad.drop('overview', 'crew', 'orig_title')

# Cria novas colunas a partir da coluna 'date'
df_trad = df_trad.withColumn('dia', df_trad.date_x.substr(1, 2))\
    .withColumn('mes', df_trad.date_x.substr(4, 2))\
    .withColumn('ano', df_trad.date_x.substr(7, 4))\
    .drop('date_x')


# Função para converter tipos de colunas
def converterColuna(dataframe, nomes, novoTipo):
    for nome in nomes:
        dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
    return dataframe


# Colunas que serão convertidas
colunas_inteiro = ['dia', 'mes', 'ano']
df_trad = converterColuna(df_trad, colunas_inteiro, IntegerType())

# df_trad.printSchema()
df_trad.show(2)
# ======================================================================


# ===> 4-Os dados tratados devem ser carregados para uma base de dados MongoDB.

# Função que cria BD no Mondo DB
def get_database():
    from pymongo import MongoClient

    # Definindo a string de conexão com o MongoDB
    CONNECTION_STRING = "mongodb+srv://root:5OkOyu53TE2xdC2i@cluster0.tnzynwo.mongodb.net"

    # Criando uma instância do cliente MongoClient com a string de conexão
    client = MongoClient(CONNECTION_STRING)

    # Retornando o banco de dados 'exer_treino_4'
    return client['exer_treino']


# Obtendo a instância do banco de dados
dbname = get_database()

# Obtendo a coleção "mod_6" dentro do banco de dados
collection_name = dbname['mod_6']

# Limitando o DataFrame a 25 linhas
df_trad = df_trad.limit(25)

# Convertendo o DataFrame para um DataFrame Pandas
df_pandas = df_trad.toPandas()

# Convertendo o DataFrame Pandas para um dicionário de registros
data_dict = df_pandas.to_dict('records')

# Inserindo os registros na coleção "data_load"
collection_name.insert_many(data_dict)

# Imprimindo uma mensagem de sucesso
print('Foi pae, agr so correr pro Abraço!')
