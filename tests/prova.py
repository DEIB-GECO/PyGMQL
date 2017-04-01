import pandas as pd

df1 = [
    {'index': 1, 'c1': 1, 'c2': 2, 'c3': 3},
    {'index': 2, 'c1': 1, 'c2': 6, 'c3': 7},
    {'index': 3, 'c1': 3, 'c2': 8, 'c3': 3},
]

df1 = pd.DataFrame.from_dict(df1)
df1 = df1.set_index('index')

df2 = [
    {'index': 1, 'c4': 2, 'c5': 2, 'c6': 4},
    {'index': 2, 'c4': 3, 'c5': 5, 'c6': 5}
]

df2 = pd.DataFrame.from_dict(df2)
df2= df2.set_index('index')

df_tot = pd.concat(objs=[df1, df2], axis=1, join='inner')

print(df_tot)

