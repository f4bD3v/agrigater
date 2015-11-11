import os
from os import path
import glob
import blaze as bz
import pandas as pd
import math
import odo
import numpy as np
import time
import shutil
from datetime import datetime, date
import calendar

### Merging: pd.merge(index_frame, df, how='outer', on=['year', 'month', 'district'])
### NOTE: this representation may be useful at some point
class DataFrameObj:
    def __init__(self, df, name):
        self.df = df
        self.name = name

def save(df, outpath):
    numerics = ['float16', 'float32', 'float64']
    if os.path.isfile(outpath):
        os.remove(outpath)
    if isinstance(df, bz.expr.split_apply_combine.By) or isinstance(df, bz.expr.expressions.Projection):
        odo.odo(df, outpath)
    else:
        num_names = list(df.select_dtypes(include=numerics).columns)
        df = df.apply(lambda x: np.round(x, decimals=3) if x.name in num_names else x)
        odo.odo(df, outpath)

def fill_records(df, date_range):
    date_df = pd.DataFrame(date_range, columns=['date'])
    grouped = df.groupby(['market', 'commodity', 'variety'])
    record_filled_groups = []
    for idx, group in grouped:
        group['date'] = pd.DatetimeIndex(group['date'])
        records = pd.merge(date_df, group, how='outer', on=['date'])
        records[['market', 'state', 'district', 'taluk', 'category', 'commodity', 'commodity_translated', 'variety']] = records[['market', 'state', 'district', 'taluk', 'category', 'commodity', 'commodity_translated', 'variety']].fillna(method='ffill').fillna(method='bfill')
        record_filled_groups.append(records)
    df = pd.concat(record_filled_groups)
    return df

def bz_compute_na_ratio(d, column):
    # d can be group or standard dataframe
    ### NOTE: to drop a column simply reassign dataframe taking a subset of columns
    if column == 'arrival':
        d = d[['date', 'market', 'arrival']]
        d = d.distinct()
    table = d[column].map(lambda x: math.isnan(x), 'string').count_values()
    nas = list(table[table[column]==True].count)
    vals = list(table[table[column]==False].count)
    ratio = 0
    if nas:
        ratio = float(nas[0])/(vals[0]+nas[0])
    #percentage = ratio * 100
    #return percentage
    return ratio

def nas_by_commodity(d, commodity, outdir):
    arrival_nas = bz_compute_na_ratio(d, 'arrival')
    ## save these figures to a spreadsheet from I can easily generate graphs!!!
    ## commodity, na_percentage_type, na_percentage
    ## or better: commodity | arrival | price
    #                rice    |  30.5%  | 20%
    modal_price_nas = bz_compute_na_ratio(d, 'modal')
    min_price_nas = bz_compute_na_ratio(d, 'min')
    max_price_nas = bz_compute_na_ratio(d, 'max')

    row = pd.DataFrame([[commodity, arrival_nas, modal_price_nas, min_price_nas, max_price_nas]], columns=['commodity', 'arrival', 'modal price', 'min price', 'max price'])
    outpath = path.join(outdir, 'nas_by_commodity.csv')
    row.to_csv(outpath, index=False)
    return

def nas_by_group(series):
    nas = series.isnull().sum() 
    if nas == 0:
        na_ratio = 1
    else:
        na_ratio = nas / len(series)
    return na_ratio

def get_group_len(series):
    return len(series)

# discard varieties to have distinct arrival values
def nas_over_time(df, commodity, outdir):
    price_df = df.groupby(['year', 'month']).agg({'min' : {'na_ratio': nas_by_group, 'len': get_group_len}, 'max' : {'na_ratio': nas_by_group, 'len': get_group_len}, 'modal' : {'na_ratio': nas_by_group, 'len': get_group_len}})
    price_df = pd.DataFrame(price_df)
    # Distinct arrivals: do not need to subset on commodity field, since all entries are identical
    df = df.drop_duplicates(subset=['date', 'market'])
    arrival_df = df.groupby(['year', 'month']).agg({'arrival': {'na_ratio': nas_by_group, 'len': get_group_len}})
    arrival_df = pd.DataFrame(arrival_df)
    # Merge stat dfs on year, month index
    commodity_df = arrival_df.merge(price_df, left_index=True, right_index=True)
    commodity_df.insert(0, 'commodity', commodity)
    outpath = path.join(outdir, 'nas_by_year-month.csv')
    #cidx_df = df['year', 'month', 'commodity']
    ### NOTE: inserting NA rows 
    # out_df = pd.merge(idx_df, commodity_df, how='outer', on=['year', 'month', 'commodity'])
    commodity_df.to_csv(outpath)#, index=False)
    return

"""
    - may be computed after estimation completed
def arrivals_per_variety(d, outdir):
    d = bz.by(d.variety, arrival=d.arrival.sum())
    outpath = path.join(outdir, 'arrivals_per_variety.csv')
    save(d, outpath)
    return

def arrivals_per_variety_over_time(d, outdir):
    d = bz.by(bz.merge(d.year, d.month, d.variety), arrival=d.arrival.sum())
    ### TODO: fill year, month gaps for plotting
    outpath = path.join(outdir, 'arrivals_per_variety_over_time.csv')
    save(d, outpath)
    return
### necessary?
"""

def arrival_over_time(d, outdir, level=None):
    df = odo.odo(d, pd.DataFrame)
    d = bz.Data(df)
    if level:
        if isinstance(level, list):
            expr = [l for l in level]
        else:
            expr = level
        dr = bz.by(bz.merge(d[expr], d.year, d.month, d.commodity), arrival=d.arrival.sum())
        outpath = path.join(outdir, 'tonnage_by_{}_year-month.csv'.format(level))
    else:
        dr = bz.by(bz.merge(d.year, d.month, d.commodity), arrival=d.arrival.sum())
        outpath = path.join(outdir, 'tonnage_by_year-month.csv')
    save(dr, outpath)
    return dr

def arrival_by_month(d, outdir, level=None):
    df = odo.odo(d, pd.DataFrame)
    d = bz.Data(df)
    if level:
        if isinstance(level, list):
            expr = [l for l in level]
        else:
            expr = level
        do = bz.by(bz.merge(d[expr], d.month, d.commodity), arrival=d.arrival.mean())
        outpath = path.join(outdir, 'tonnage_by_{}_month.csv'.format(level))
    else:
        do = bz.by(bz.merge(d.month, d.commodity), arrival=d.arrival.mean())
        outpath = path.join(outdir, 'tonnage_by_month.csv')
    save(do, outpath)
    return

def arrival_by_year(d, outdir, level=None):
    df = odo.odo(d, pd.DataFrame)
    d = bz.Data(df)
    do = bz.by(bz.merge(d.year, d.commodity), arrival=d.arrival.sum())
    if level:
        if isinstance(level, list):
            expr = [l for l in level]
        else:
            expr = level
        do = bz.by(bz.merge(d[expr], d.year, d.commodity), arrival=d.arrival.sum())
        outpath = path.join(outdir, 'tonnage_by_{}_year.csv'.format(level))
    else:
        do = bz.by(bz.merge(d.year, d.commodity), arrival=d.arrival.sum())
        outpath = path.join(outdir, 'tonnage_by_year.csv')
    save(do, outpath)
    return

def arrival_by_level(d, outdir, level):
    dr = bz.by(bz.merge(d[level], d.commodity, d.year, d.month), arrival=d.arrival.sum())
    outpath = path.join(outdir, 'tonnage_by_{}.csv'.format(level))
    do = bz.by(bz.merge(d[level], d.commodity), arrival=d.arrival.sum())
    #d[[level, 'commodity']]
    save(do, outpath)
    by_year_month = arrival_over_time(dr, outdir, level)
    arrival_by_year(dr, outdir, level)
    arrival_by_month(by_year_month, outdir, level)
    return 

### need additional group by and sum after appending has produced figures for all markets
def arrival_by_market(d, outdir):
    do = bz.by(bz.merge(d.market, d.commodity), arrival=d.arrival.sum())
    outpath = path.join(outdir, 'tonnage_by_market.csv')
    save(do, outpath)
    """
    by_year_month = arrival_over_time(do, outdir)
    arrival_by_time_unit(do, outdir, 'year')
    arrival_by_time_unit(by_year_month, outdir, 'month')
    """
    return

def arrival_by_district(d, outdir):
    do = bz.by(bz.merge(d.market, d.commodity), arrival=d.arrival.sum())
    outpath = path.join(outdir, 'tonnage_by_district.csv')
    save(do, outpath)
    return

def arrival_by_state(d, outdir):
    do = bz.by(bz.merge(d.market, d.state), arrival=d.arrival.sum())
    outpath = path.join(outdir, 'tonnage_by_state.csv')
    save(do, outpath)
    return

# data coverage global by commodity
# data coverage local(market) by commodity

# http://stackoverflow.com/questions/15322632/python-pandas-df-groupby-agg-column-reference-in-agg
def get_group_coverage(df, date_range=False, loc=True):
    # can I do != np.nan?
    #df = df[(df['arrival'] != '-') | (df['min'] != '-') | (df['max'] != '-') | (df['modal'] != '-')]
    series = df['date']
    if not loc:
        series = series.unique()
    if not isinstance(date_range, pd.DatetimeIndex):
        max_date = pd.Timestamp(series.max()).to_pydatetime()
        min_date = pd.Timestamp(series.min()).to_pydatetime()
        max_month = max_date.month
        max_year = max_date.year
        min_month = min_date.month
        min_year = min_date.year
        #min_days = calendar.monthrange(min_year, min_month)
        max_days = calendar.monthrange(max_year, max_month)
        first_date = date(min_year, min_month, 1)
        last_date = date(max_year, max_month, max_days[1])
        date_range = pd.date_range(first_date, last_date)
    boolarr = pd.Series(np.in1d(date_range, pd.DatetimeIndex(series)))
    counts = boolarr.value_counts()
    ### TODO: only print zero if only row name is False
    count = 0 if (len(counts) == 1 and list(counts.index) == [False]) else counts.loc[True] 
    coverage = count / len(date_range)
    # 'district': df[group_col].unique()[0], 
    coverage = pd.Series({'coverage' : coverage}) # index=[df[group_col].unique()[0]])
    return coverage

def get_coverage(df, date_range, outdir, group_cols = []):
    res_df = None
    if group_cols:
        grouped = df.groupby(group_cols)
        res_df = grouped.apply(lambda x: get_group_coverage(x, date_range, False))
    else:
        dates = df['date'].unique()
        commodity_cover = len(dates)/len(date_range)
        res_df = pd.DataFrame([[commodity_cover]], columns=['coverage'])
        group_cols = ['commodity']

    if res_df.index.name:
        res_df.reset_index(inplace=True)
    commodity = df['commodity'].unique()[0]
    res_df.insert(0, 'commodity', commodity)
    outpath = path.join(outdir, 'coverage_by_{}.csv'.format('-'.join(group_cols)))
    save(res_df, outpath)
    return

def get_loc_coverage(df, subset_cols, date_group_cols, group_cols, date_range, outdir, size_col=None):
    distinct_df = df.drop_duplicates(subset=subset_cols)
    grouped = distinct_df.groupby(date_group_cols + group_cols)
    res1 = grouped.apply(lambda x: get_group_coverage(x, date_range)) #aggregate(lambda col: get_coverage(x, date_range)).date)
    if size_col:
        size_df = df.drop_duplicates(subset=['date', size_col])
        grouped = size_df.groupby(date_group_cols + group_cols)
        res2 = pd.DataFrame(grouped.size(), columns=['records']) # same as df['market'].value_counts()?
    else:
        res2 = pd.DataFrame(grouped.size(), columns=['records'])
    res1.reset_index(inplace=True)
    res2.reset_index(inplace=True)
    merged_df = pd.merge(res1, res2, how="outer", on=date_group_cols + group_cols)
    #merged_df = res1.merge(res2, left_index=True, right_index=True)
    #merged_df.reset_index(inplace=True)
    commodity = df['commodity'].unique()[0]
    merged_df.insert(0, 'commodity', commodity)
    ### now set reset_index and add commodity column
    outpath = path.join(outdir, 'coverage_by_{}.csv'.format('-'.join(date_group_cols + [group_cols[-1]])))
    save(merged_df, outpath)
    if len(date_group_cols + group_cols) > 2:
        return merged_df 
    else:
        return

def mean_by_month(df, outdir, level):
    ### TODO: include year range
    df.drop('year', axis=1, inplace=True)
    df = df.groupby('month').mean()
    df.reset_index(inplace=True)
    outpath = path.join(outdir, 'coverage_by_{}-month.csv'.format(level))
    save(df, outpath)
    return

def get_coverages(df, date_range, outdir, commodity):
 
    get_coverage(df, date_range, outdir)
    get_coverage(df, None, outdir, ['year'])
    get_coverage(df, None, outdir, ['month'])
    get_coverage(df, None, outdir, ['year', 'month'])
    # get total coverage

    ### TODO: at this point it would be nice to record statistics at district and state level
    # => record stats for aggregation planning
    ### NOTE: data frame contains all entries for specific commodity
    ### NOTE: use one record per date per market
    ### columns on which to drop duplicates for distinct date entries per market:
    market_cols = ['date', 'market']
    get_loc_coverage(df, market_cols, [], ['state', 'district', 'market'], date_range, outdir)
    get_loc_coverage(df, ['date', 'district'], [], ['state', 'district'], date_range, outdir, 'market')
    get_loc_coverage(df, ['date', 'state'], [], ['state'], date_range, outdir, 'market')

    get_loc_coverage(df, market_cols, ['year'], ['state', 'district', 'market'], None, outdir)
    get_loc_coverage(df, ['date', 'district'], ['year'], ['state', 'district'], None, outdir, 'market')
    get_loc_coverage(df, ['date', 'state'], ['year'], ['state'], None, outdir, 'market')

    ym_market = get_loc_coverage(df, market_cols, ['year', 'month'], ['state', 'district', 'market'], None, outdir)
    ym_district = get_loc_coverage(df, ['date', 'district'], ['year', 'month'], ['state', 'district'], None, outdir, 'market')
    ym_state = get_loc_coverage(df, ['date', 'state'], ['year', 'month'], ['state'], None, outdir, 'market')
    # on date X 7 distinct arrival entries, 12 distinct price entries for a commodity
    #(comm_cover, year_comm_cover, year_month_comm_cover), (year_market_cover, year_district_cover, year_state_cover)

    ### TODO: by month mean coverage
    ### additional group by month: and call avg
    mean_by_month(ym_market, outdir, 'market')
    mean_by_month(ym_district, outdir, 'district')
    mean_by_month(ym_state, outdir, 'state')
    return


### TODO: compute some basic intro statistics for the analysis part of the thesis here 
# --> more complicated stuff may follow with R?
def compute_stats(data_dir, filename):
    ### PROBLEM: some commodities are still spread over multiple files => solve by gathering stats over multiple files
    csvr = bz.resource(filename)
    ds = bz.discover(csvr)

    d = bz.Data(filename, dshape=ds)
    category = list(d.category)[0]
    commodity = list(d.commodity)[0]
    d = bz.transform(d, year=d.date.year, month=d.date.month)
    outdir = path.join(data_dir, 'stats', category, commodity)
    if not path.isdir(outdir):
        os.makedirs(outdir)

    nas_by_commodity(d, commodity, outdir)

    ### NOTE: transform: add date and month column
    # problem here is that it's not simply calling a predefined function
    ### NEED PANDAS FOR THIS
    #bz.by(bz.merge(d.year, d.month), arrival=d.arrival.sum())
    df = odo.odo(d, pd.DataFrame)

    # TODO: read the latest date from config/or use yesterdays date
    date_range = pd.date_range('1/1/2002', time.strftime('%m/%d/%Y'))
    #df = fill_records(df, date_range)

    ### In what particular periods do NAs occur?
    # does data quality improve over time? na ratio per (year, month) => further group by commodity before saving final dataframe
    nas_over_time(df, commodity, outdir)

    ### NOTE: taking into account that arrivals I have not taken into account that arrivals are repeated
    arrivals = d[['date', 'state', 'district', 'market', 'commodity', 'arrival', 'year', 'month']].distinct()
    arrival_by_year_month = arrival_over_time(arrivals, outdir)
    arrival_by_year(arrivals, outdir)
    arrival_by_month(arrival_by_year_month, outdir)

    arrival_by_level(arrivals, outdir, ['state', 'district', 'market'])
    arrival_by_level(arrivals, outdir, ['state', 'district'])
    arrival_by_level(arrivals, outdir, 'state')
    """
    arrival_by_month(arrival_by_year_month, outdir)
    arrival_by_year(arrivals, outdir)
    # arrival by state,( district,) market
    arrival_by_market(arrivals, outdir) 
    arrival_by_district(arrivals, outdir) 
    arrival_by_state(arrivals, outdir) 
    """

    get_coverages(df, date_range, outdir, commodity) 
    ### TODO:
    # display some of these statistics in the application
    """
    make nice printout
    to compute arrival stats by commodity, first have to drop variety column and then remove all duplicates
    df.drop_duplicates() --> df.distinct()
    trick to work with blaze: fill unwanted columns (variety) with unique string/float and call drop duplicates on dataframe

    Questions per commodity (can be answered with single files):
        What is the variety with the most arrivals per commodity? => arrivals per variety
        What markets have the most arrivals per commodity?
        What months have the most arrivals per commodity?
        What weekdays have the most arrivals per commodity?
        Average price per commodity?
            - by year
            - by month (seasonal phenomena?)
            - by year, month
        What is the NA ratio of arrivals? Are there any particular patterns to this ratio?
            - by month? => DONE
            - by market? --> COULD USE THIS TO VISUALIZE DATA QUALITY!

    Questions that can't be answered with single files => use pymongo or odo to load multiple collections into dataframe?
    - arrival tonnage by commodity by(commodity, ==> arrival.sum()) (total)
    - arrival tonnage by category?
    - avg price by commodity by(commodity, ==> modal.mean())
    - minimum modal price by commodity 
    - maximum modal price by commodity
    - arrival tonnages by years (total)
    - arrival tonnages by months (accumulative)
    - arrival tonnages by commodity,years (total)
    - arrival tonnages by commodity, month (accumulative)
    - arrival tonnages by commodity, month, year
    
    TODO: test all of these in ipython first
    by(df.commodity, total_arrivals = df.arrival.sum())

    - NA percentage for tonnages of commodity

    - NA percentage for tonnages of commodity
    """
    return

### TODO: how to aggregate coverage?
def arrival_aggregation(data_dir):
    all_dir = path.join(data_dir, 'stats', 'all')
    os.chdir(all_dir)
    files = glob.glob('*.csv')
    files = filter(lambda x: 'tonnage' in x and not 'commodity' in x, files) 
    ### -
    for filename in files:
        group_col = filename.rstrip('.csv').split('_')[-1]
        df = pd.DataFrame.from_csv(filename, index_col = None)
        df.groupby(group_col).arrival.sum()
    return

def combine_commodity_stats(data_dir):
    outdir = path.join(data_dir, 'stats')
    folders = os.listdir(outdir)
    all_dir = path.join(outdir, 'all')
    if path.isdir(all_dir):
        shutil.rmtree(all_dir)
    os.makedirs(all_dir)
    for folder in folders:
        if os.path.isfile(folder):
            continue
        folder_dir = path.join(outdir, folder)
        os.chdir(folder_dir)
        files = glob.glob('*.csv')
        for filename in files:
            print(filename)
            ### TODO: check to see if this works
            if 'variety' in filename:
                continue
            outdir = path.join(all_dir, 'stacked')
            if not path.isdir(outdir):
                os.makedirs(outdir)
            outpath = path.join(outdir, filename)
            os.system('/bin/bash -c \"cat {0} >> {1}\"'.format(filename, outpath))
    # delete *all.csv files
    # for every category, commodity read files
    # and execute bash append command
    return

def main():
    data_dir = '../../data'
    os.chdir(data_dir)
    data_dir = os.getcwd()
    src_dir = path.join('agmarknet', 'by_commodity')
    init_dir = os.getcwd()
    os.chdir(src_dir)
    src_dir = os.getcwd()
    folders = glob.glob('*')
    #title = ''
    #start = time.time()
    ### TODO: make this one function call?
    #combine_commodity_stats(data_dir)
    for folder in folders:
        os.chdir(path.join(folder, 'integrated'))
        files = glob.glob('*.csv')
        for filename in files:
            print('Computing stats for {}'.format(filename))
            compute_stats(data_dir, filename)
            ### TODO: write unpack and print function!
        os.chdir(src_dir)
    os.chdir(init_dir)

    combine_commodity_stats(data_dir)
    arrival_aggregation(data_dir)
    return

if __name__ == "__main__":
    main()
