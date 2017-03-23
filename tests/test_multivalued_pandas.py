import pandas as pd

data = {
    'id1' : {'attr1': ['val1','val2','val3'], 'attr2': ['val3'], 'attr5':['val1', 'val4']},
    'id2' : {'attr1': ['val2','val3'], 'attr2': ['val4'], 'attr5':['val1', 'val4']},
    'id3' : {'attr1': ['val1','val3'], 'attr5':['val1', 'val4']},
    'id4' : {'attr1': ['val2'], 'attr2': ['val3'], 'attr3':['val2', 'val3']}
}

df = pd.DataFrame.from_dict(data, orient='index')

print(df)

print("Select only the rows of the df for which in attr1 there is val1")
print(df[df['attr1'].apply(lambda x: "val1" in x).values])
