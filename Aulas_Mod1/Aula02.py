#   Aula 02 - Series

# import numpy as np
import pandas as pd

notas = pd.Series([10, 5, 7.5, 9, 10])
print(notas, "\n\n\n")
print(notas.values, "\n\n\n")
print(notas.index, "\n\n\n")


notas2 = pd.Series(
    [10, 5, 7.5, 9, 10], index=["José", "Carlos", "André", "Pedro", "Maria"]
)  # Atribuir Indices
print(notas2)

print(notas.describe())  # descreve dados estatísticos de uma Serie/DataFrame
print(notas.mean())  # mean é a media
print(notas**2)  # ** elevado o valor a 2
