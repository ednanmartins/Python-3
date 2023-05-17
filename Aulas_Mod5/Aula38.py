# Aula 38 - Data visualization com Matplotlib – Gráfico de barras

import pandas as pd
import matplotlib.pyplot as plt

iris = pd.read_csv('iris.csv', names=[
                   'sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class'])
wine_reviews = pd.read_csv('winemag-data-130k-v2.csv', index_col=0)


# Criando uma figura e um conjunto de eixos para o gráfico
fig, ax = plt.subplots()

# Contando a frequência de ocorrência de cada valor na coluna 'style' do DataFrame 'wine_reviews'
data = wine_reviews['style'].value_counts()

# Obtendo os rótulos dos estilos de vinho
style = data.index

# Obtendo as frequências de ocorrência dos estilos de vinho
frequency = data.values

# Criando um gráfico de barras utilizando os estilos de vinho e suas frequências
ax.bar(style, frequency)

# Definindo o título do gráfico
ax.set_title('Wine Review Scores')

# Definindo o rótulo do eixo x
ax.set_xlabel('Style')

# Definindo o rótulo do eixo y
ax.set_ylabel('Frequency')
