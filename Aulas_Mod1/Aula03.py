#   Aula 03 - DataFrame

# import numpy as np
import pandas as pd

df = pd.DataFrame(
    {
        "Aluno": ["José", "Carlos", "Ana", "Júlia", "Débora"],
        "Faltas": [3, 4, 2, 4, 3],
        "Prova": [2, 7, 8, 5.5, 9.2],
        "Seminário": [8.5, 5, 8.2, 6, 9.5],
    }
)
print(df)
