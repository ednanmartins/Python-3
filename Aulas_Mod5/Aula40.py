# Aula 40 - Data visualization com Matplotlib – Gráfico de linha

import pandas as pd
import matplotlib.pyplot as plt

iris = pd.read_csv('iris.csv', names=[
                   'sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class'])
wine_reviews = pd.read_csv('winemag-data-130k-v2.csv', index_col=0)

# Obtendo as colunas do DataFrame 'iris', excluindo a coluna 'class'
columns = iris.columns.drop(['class'])

# Criando uma lista de valores para o eixo x
# '.shape' diz o tamanho do Dataset
x_data = range(0, iris.shape[0])


# Criando uma figura e um conjunto de eixos para o gráfico
fig, ax = plt.subplots()

# Iterando sobre as colunas do DataFrame 'iris'
for column in columns:
    # Plotando um gráfico de linha para cada coluna em relação aos valores de x
    ax.plot(x_data, iris[column], label=column)


# Definindo o título do gráfico
ax.set_title('Iris Dataset')

# Adicionando a legenda ao gráfico
ax.legend()
