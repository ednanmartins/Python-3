'''Leia o dataset 'supermarket_sales.csv' disponível nesta plataforma,
e plot pelo menos um tipo de gráfico de cada biblioteca usada neste módulo.'''

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

# Lendo o dataset
supermarket = pd.read_csv('supermarket_sales.csv')

# Dando uma olhada nos dados
# print(supermarket.head())


# =====> Pandas <=====
# Gráfico de linhas (DataFrame.plot.line)
supermarket['Rating'].plot.line()

# Gráfico de Área (DataFrame.plot.area)
supermarket[['cogs', 'gross income']].plot.area()

# Gráfico de barras (DataFrame.plot.bar)
supermarket['Product line'].value_counts().plot.bar()

# Gráfico de barras empilhadas (DataFrame.plot.barh(stacked=True))
supermarket.groupby(['City', 'Payment'])[
    'Total'].sum().unstack().plot.bar(stacked=True)

# Gráfico de dispersão (DataFrame.plot.scatter)
supermarket.plot.scatter(x='Unit price', y='Quantity',
                         title='Gragico de Disperção')

# Gráfico de pizza (DataFrame.plot.pie)
supermarket['Payment'].value_counts().plot.pie()
# -------------------------------------------------------------

# =====> Seaborn <=====
# Gráfico de dispersão (sns.scatterplot)
sns.scatterplot(x='Unit price', y='Product line', data=supermarket)

# Gráfico de histograma (sns.histplot)
sns.histplot(supermarket['Unit price'], bins=10, kde=True)

# Grafico de Contagem (sns.countplot)
sns.countplot(supermarket['Quantity'])
# -------------------------------------------------------------

# =====> Matplotlib <=====
# Gráfico de linhas (plt.plot)
plt.plot(supermarket['Date'], supermarket['Total'])

# Gráfico de barras (plt.bar)
# Gráfico de barras horizontais (plt.barh)
# Gráfico de dispersão (plt.scatter)
# Gráfico de pizza (plt.pie)
# Gráfico de área (plt.fill_between)
# -------------------------------------------------------------

# =====> Plotly <=====
# Gráfico de linhas (px.Line)
fig = px.line(supermarket, x='Date', y='Total', title='Grafico com Plotly')
fig.show()
# Gráfico de barras (go.Bar)
# Gráfico de barras empilhadas (go.Bar(marker={'color': 'stack'}))
# Gráfico de dispersão (go.Scatter)
# Gráfico de pizza (go.Pie)
# Gráfico de área (go.Area)
