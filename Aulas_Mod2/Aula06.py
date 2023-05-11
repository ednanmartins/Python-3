#   Aula 06 - Importando CSV

import numpy as np
import pandas as pd

df_csv = pd.read_csv('Aulas_Mod2/dados.csv')
# print(df_csv)
print(df_csv.head(3))  # .head imprime as primeiras x linhas
print(df_csv.tail(3))  # .tail imprime as ultimas x linhas
