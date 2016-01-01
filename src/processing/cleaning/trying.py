# coding: utf-8
import pandas as pd
import blaze as bz
df = pd.DataFrame.from_csv('Tea_stacked_cleaned.csv', index_col=None)
df.head()
d = bz.Data('Tea_stacked_cleaned.csv')
d.shape
d.dshape
d = bz.transform(d, year=d.date.year, month=d.date.month)
d.head(5)
d.tail(5)
import odo
df = odo.odo(d, pd.DataFrame)
df.head(2)
df.groupby(['year', 'month'])
df.groupby(['year', 'month']).arrival
groups = df.groupby(['year', 'month'])
print(groups)
df.groupby(['year', 'month']).arrival.sum()
df.loc[df.year == 15]
df.loc[:, df.year==15]
grouped = df.groupby(['year', 'month'])
for name, group in grouped:
    print(name)
    print(group)
    
get_ipython().magic('save trying.py 1-23')
