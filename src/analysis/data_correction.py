import yaml
from mongo_handler import MongoDB
import pandas as pd

config = yaml.load(open('../db/db_config.yaml', 'r'))
db = MongoDB(config['meteordb'], config['address'], config['meteorport'])

coll_name = "market.wheat.varieties"
cursor = db.fetch(coll_name, {"market": "Narela", "variety": "Mexican"}, False)
df = pd.DataFrame(list(cursor))
df = df.sort('date')
print(df.head())
df_price_sub = df.loc[df["modalPrice"] > 100] 
print(df_price_sub)
print(df['modalPrice'].diff())
#df_tonnage_sub = df.loc[df["commodityTonnage"] > 100] 

