# Aula 35 - Introdução a data visualization com Python

import pandas as pd

# Lendo o arquivo 'iris.csv' e atribuindo os nomes das colunas
iris = pd.read_csv('iris.csv', names=[
                   'sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class'])


# Imprimindo as primeiras linhas do DataFrame 'iris'
print(iris.head())


# Lendo o arquivo 'winemag-data-130k-v2.csv' com um índice específico
wine_reviews = pd.read_csv('winemag-data-130k-v2.csv', index_col=0)

# Imprimindo as primeiras linhas do DataFrame 'wine_reviews'
wine_reviews.head()
