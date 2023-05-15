'''EXERCÍCIO DE TREINO*

Use a base de dados disponibilizada, “adult_data.csv”, e realize as seguintes etapas:

·    Importe as bibliotecas necessárias;

·    Importe os tipos de dados necessários;

·    Importe as funções SQL;

·    Crie uma sessão PySpark;

·    Defina um esquema de tratamento (Não esqueça de analisar visualmente o dataset antes);

·    Importe o dataset;

Após essas etapas, faça os seguintes tratamentos:

1.    Imprima o Schema de seu DF;

2.    Imprima os primeiros 5 linhas de seu DF;

3.    Converta o campo idade do tipo inteiro para o tipo Float (ou vice versa, dependendo do seu Schema);

4.    Exiba somente 5 itens com os campos ‘age’ e ‘education’;

5.    Agrupe a quantidade de itens em ‘education’ ordenados de maneira ascendente;

6.    Exiba um describe da tabela ‘capital_gain’;'''

from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    StructField, StructType, IntegerType, StringType, DoubleType)

# Cria uma instância do SparkSession
spark = SparkSession.builder \
    .appName('fisrtSession') \
    .config('spark.master', 'local[4]') \
    .config('spark.executor.memory', '1gb') \
    .config('spark.shuffle.partitions', 1) \
    .getOrCreate()

# Teste que fiz para ver como tava antes, top
# df = spark.read.csv('adult_data.csv')
# df.show(3)

schema = StructType([
    StructField("ID", IntegerType()),
    StructField("Cod", IntegerType()),
    StructField("Tipo", StringType()),
    StructField("Identificação", IntegerType()),
    StructField("Vou chamar de Hrs", StringType()),
    StructField("Outro num", IntegerType()),
    StructField("Estado Civil", StringType()),
    StructField("Profissão", StringType()),
    StructField("Parentesco", StringType()),
    StructField("Cor aleatoria", StringType()),
    StructField("Genero", StringType()),
    StructField("Mais num", DoubleType()),
    StructField("Num 0", DoubleType()),
    StructField("Acho que Idade", IntegerType()),
    StructField("Cidade", StringType()),
    StructField("+/- 50k", StringType())
])

# Atribuindo Schema selecionado
df = spark.read.schema(schema).csv('adult_data.csv')

# Questão 1-Imprima o Schema de seu DF:
df.printSchema()
# ------------------------------------------------------

# Questão 2-Imprima os primeiros 5 linhas de seu DF:
df.show(5)
# ------------------------------------------------------

# Questão 3-Converta o campo idade do tipo inteiro para o tipo Float (ou vice versa, dependendo do seu Schema):
nomeColuna = 'Acho que Idade'
novoTipo = StringType()
new_df = df.withColumn(nomeColuna, df[nomeColuna].cast(novoTipo))

new_df.printSchema()
# ------------------------------------------------------

# Questão 4-Exiba somente 5 itens com os campos ‘age’ e ‘education’:
age = 'Outro num'
education = 'Vou chamar de Hrs'
df2 = df.select(age, education)
df2.show(5)
# ------------------------------------------------------

# Questão 5-Agrupe a quantidade de itens em ‘education’ ordenados de maneira ascendente:
df2.groupBy(education).count().sort(education, ascendent=True).show()
# ------------------------------------------------------

# Questão 6-Exiba um describe da tabela ‘capital_gain’:
capital_gain = 'Cidade'
df.describe(capital_gain)
# ------------------------------------------------------
