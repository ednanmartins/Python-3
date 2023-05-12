#   Aula 18 - Abordando colunas

from pyspark.sql import SparkSession
from pyspark.sql import Row
# from pyspark.sql.types import *
import findspark
findspark.init()

spark = SparkSession.builder \
    .master('local') \
    .appName('Aula que Tive que Adivinhar PySpark') \
    .config('spark.executor.memory', '1gb') \
    .getOrCreate()

# Ele ja tava com esse codigo quando cheguei
'''sc = spark.sparkContext
rdd = sc.textFile('Aulas_Mod4/Salary_Data.csv')
rdd_new = rdd.map(lambda line: line.splir(','))'''


# Video do Youtube
'''df = spark.read.csv('Aulas_Mod4/Salary_Data2.csv',
                    header=True, inferSchema=True)
df.printSchema()'''

sc = spark.sparkContext
rdd = sc.textFile('Salary_Data.csv')
rdd = rdd.map(lambda line: line.split(","))

df = rdd.map(lambda line: Row(AnosExperiencia=line[0], Salario=line[1])).toDF()
df.show()
