#   Aula 05 - Exibição de dados

import numpy as np
import pandas as pd

df = pd.DataFrame({'Aluno': ["José", "Carlos", "Ana", "Júlia", "Débora"],
                   'Faltas': [3, 4, 2, 4, 3],
                   'Prova': [2, 7, 8, 5.5, 9.2],
                   'Seminário': [8.5, 5, 8.2, 6, 9.5]})
# print(df)
# print(df.dtypes)
# print(df.columns)
# print(df['Aluno'])
# print(df.describe())
# print(df.sort_values(by="Seminário"))   #sort significa 'ordenado por'
# print(df.loc[3])    #loc mostra por indice
# print(df[df["Seminário"] > 8.0])
print(df[(df["Seminário"] > 8.0) & (df["Prova"] > 3)])
