# Aula 37 - Data visualization com Seaborn

import pandas as pd
import seaborn as sns

# Lendo o arquivo 'iris.csv' e atribuindo os nomes das colunas
iris = pd.read_csv('iris.csv', names=[
                   'sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class'])

# Lendo o arquivo 'winemag-data-130k-v2.csv' com um índice específico
wine_reviews = pd.read_csv('winemag-data-130k-v2.csv', index_col=0)


# Plotando um gráfico de dispersão utilizando o seaborn
# 'sns.scatterplot()' cria um gráfico de dispersão
# 'x=' e 'y=' define a coluna como o eixo x e y
# data=iris especifica o DataFrame 'iris' como fonte de dados
sns.scatterplot(x='sepal_length', y='sepal_width', data=iris)


# Plotando um histograma utilizando o seaborn
# 'sns.displot()' cria um histograma
# wine_reviews['volatile_acidity'] define a coluna
# bins=10 define o número de intervalos (bins) no histograma
# kde=True ativa a estimativa de densidade do kernel (Kernel Density Estimation - KDE)
sns.displot(wine_reviews['volatile_acidity'], bins=10, kde=True)


# Plotando um gráfico de contagem utilizando o seaborn
# 'sns.countplot()' cria um gráfico de contagem de valores
# wine_reviews['volatile_acidity'] define a coluna 'volatile_acidity' como a variável de interesse
sns.countplot(wine_reviews['volatile_acidity'])
