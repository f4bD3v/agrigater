import yaml
import numpy as np
import pandas as pd
# from monary import Monary
import time
import odo
# http://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.sparse.linalg.lsqr.html

# http://glowingpython.blogspot.ch/2012/03/solving-overdetermined-systems-with-qr.html

### TODO: infer modal price from min or max?
### for arrival: some kind of parameter optimization

"""
def monary_retrieve(config, coll):
    # http://alexgaudio.com/2012/07/07/monarymongopandas.html
    start = time.time()
    mon = Monary("127.0.0.1:3001")
    columns = ['date', 'market', 'arrival']
    np_arrs = mon.query('meteor', coll, {}, columns, ['date', 'string', 'float64'])
    df = np.matrix(np_arrs).transpose() 
    df = pd.DataFrame(df, columns=columns)
    end = time.time()
    print(df.head())
    print(end - start)
    return df
"""

### TODO:
#  http://stats.stackexchange.com/questions/144924/missing-data-and-imputation-in-general
# Approach 1: discard all records that have missing commodityTonnage => compute aggregation => then dump database to csvs -- strike this
# Approach 2: time-aware linear interpolation mixed with location aware mean => compute aggregation => dump database to csvs
# Approach 3:
# for state: fetch all markets with entries for a specific commodity
# retrieve commodityTonnage series and outer join them into a series: this will introduce NAs for missing records, however, we only want to impute missing values
# create such a dataframe and see what it looks like
## then find a clever way to do multiple imputation only for cells that are missing data and not part of a missing record
#def retrieve_markets():
#    db.fetch({ market: 1})

def odo_retrieve(config, coll):
    start = time.time()
    mongo_str = 'mongodb://{0}:{1}/{2}::'.format(config['address'], config['meteorport'], config['meteordb'])
    mongo_src = mongo_str + coll
    print(mongo_src)
    df = odo.odo(mongo_src, pd.DataFrame)
    end = time.time()
    print(df.head())
    print(end - start)
    return df

def main():
    config = yaml.load(open('../db/db_config.yaml', 'r'))
    coll = 'market_wheat_varieties'
    df = odo_retrieve(config, coll)
    df = df.drop_duplicates(subset=['date', 'market'])
    df = df.set_index('date')
    print(df.head())
    df['commodityTonnage'] = df['commodityTonnage'].interpolate(method='time')
    print(df.head())

if __name__ == "__main__":
    main()