# Aula 36 - Data visualization com Pandas

import pandas as pd

# Lendo o arquivo 'iris.csv' e atribuindo os nomes das colunas
iris = pd.read_csv('iris.csv', names=[
                   'sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class'])

# Lendo o arquivo 'winemag-data-130k-v2.csv' com um índice específico
wine_reviews = pd.read_csv('winemag-data-130k-v2.csv', index_col=0)

# Plotando um grafico de dispersão
# 'plot' significa q isso fará um grafico com os dados
# 'scatter' é o tipo de grafico que iremos formar
iris.plot.scatter(x='sepal_length', y='sepal_width', title='Iris Dataset')

# Plotando um histograma
# 'volatile_acidity' é a tabela de dados selecionada
# 'hist' é o tipo de grafico selecionado
wine_reviews['volatile_acidity'].plot.hist()

# Plotando um gráfico de barras
wine_reviews['style'].value_counts().sort_index().plot.bar()
