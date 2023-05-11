# Aula 07 - Manipulação de dados

import numpy as np
import pandas as pd

df_csv = pd.read_csv("Aulas_Mod2\dados.csv")


# print(df_csv['bairro'].unique())  # imprime coluna selecionada sem repetição
# print(df_csv['bairro'].value_counts()) # imprime a quantidade de cada dados da coluna
# print(df_csv.groupby("bairro").mean())  # agrupa por coluna selecionada

# ------------------------------------

df2 = df_csv.head()
# print(df2)

# replace serve para substituir um valor. pm2 é a coluna. valorantigo:novo_val
df3 = df2.replace({'pm2': {12031.25: np.nan}})
print(df3)

df4 = df3.dropna()  # remove erro .nan
print(df4)
