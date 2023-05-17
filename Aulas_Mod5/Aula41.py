# Aula 41 - Data visualization com Matplotlib – Gráfico de scatter

import pandas as pd
import matplotlib.pyplot as plt

iris = pd.read_csv('iris.csv', names=[
                   'sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class'])
wine_reviews = pd.read_csv('winemag-data-130k-v2.csv', index_col=0)


# Criando uma figura e um conjunto de eixos para o gráfico
fig, ax = plt.subplots()

# Criando um gráfico de dispersão (scatter plot) com os valores de 'sepal_length' no eixo x e 'sepal_width' no eixo y
ax.scatter(iris['sepal_length'], iris['sepal_width'])


# Definindo titulos
ax.set_title('Iris Dataset')
ax.set_xlabel('sepal_length')
ax.set_ylabel('sepal_width')
