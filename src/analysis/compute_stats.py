import os
from os import path
import sys
import glob
import blaze as bz
import pandas as pd
import math
import odo
import numpy as np
import time
import json
import shutil
from datetime import datetime, date
import calendar
import ast

### Merging: pd.merge(index_frame, df, how='outer', on=['year', 'month', 'district'])
### NOTE: this representation may be useful at some point
class DataFrameObj:
    def __init__(self, df, name):
        self.df = df
        self.name = name

replace = True

def df_round(df, decimals=3):
    numerics = ['float16', 'float32', 'float64']
    num_names = list(df.select_dtypes(include=numerics).columns)
    df = df.apply(lambda x: np.round(x, decimals=decimals) if x.name in num_names else x)
    return df

def save(df, outpath, replace):
    print('Writing to {}'.format(outpath.split('/')[-1]), replace)
    if replace and path.exists(outpath):
        os.remove(outpath)
    if isinstance(df, bz.expr.split_apply_combine.By) or isinstance(df, bz.expr.expressions.Projection):
        print(bz.compute(df.head()))
        odo.odo(df, outpath, ds=df.dshape)
    else:
        df = df_round(df)
        df.to_csv(outpath, index=False)
        #odo.odo(df, outpath)
    return

def fill_records(df, date_range):
    date_df = pd.DataFrame(date_range, columns=['date'])
    grouped = df.groupby(['market', 'commodity', 'variety'])
    record_filled_groups = []
    for idx, group in grouped:
        group['date'] = pd.DatetimeIndex(group['date'])
        records = pd.merge(date_df, group, how='outer', on=['date'])
        records[['market', 'state', 'district', 'taluk', 'category', 'commodity', 'commodityTranslated', 'variety']] = records[['market', 'state', 'district', 'taluk', 'category', 'commodity', 'commodityTranslated', 'variety']].fillna(method='ffill').fillna(method='bfill')
        record_filled_groups.append(records)
    df = pd.concat(record_filled_groups)
    return df

def bz_compute_na_ratio(d, column):
    # d can be group or standard dataframe
    ### NOTE: to drop a column simply reassign dataframe taking a subset of columns
    if column == 'commodityTonnage':
        d = d[['date', 'market', 'commodityTonnage']]
        d = d.distinct()
    table = d[column].map(lambda x: math.isnan(x), 'string').count_values()
    nas = list(table[table[column]==True].count)
    """
    vals = list(table[table[column]==False].count)
    ratio = 0
    if nas:
        ratio = float(nas[0])/(vals[0]+nas[0])
    #percentage = ratio * 100
    #return percentage
    return ratio
    """
    return nas

def nas_by_commodity(d, commodity, outdir):
    outpath = path.join(outdir, 'nas_by_commodity.csv')
    if replace or not path.exists(outpath):
        commodityTonnage_nas = bz_compute_na_ratio(d, 'commodityTonnage')
        ## save these figures to a spreadsheet from I can easily generate graphs!!!
        ## commodity, na_percentage_type, na_percentage
        ## or better: commodity | commodityTonnage | price
        #                rice    |  30.5%  | 20%
        modal_price_nas = bz_compute_na_ratio(d, 'modal')
        min_price_nas = bz_compute_na_ratio(d, 'min')
        max_price_nas = bz_compute_na_ratio(d, 'max')

        row = pd.DataFrame([[commodity, commodityTonnage_nas, modal_price_nas, min_price_nas, max_price_nas]], columns=['commodity', 'commodityTonnage', 'modal', 'min', 'max'])
        #row = df_round(row)
        row.to_csv(outpath, index=False)
    return

def nas_by_group(series):
    nas = series.isnull().sum()
    #na_ratio = nas / len(series)
    #return na_ratio
    return nas

def get_group_len(series):
    return len(series)

def get_loc_nas(df, commodity, date_group_cols, group_cols, outdir):
    name = '-'.join(group_cols)
    time = '-'.join(date_group_cols)
    if date_group_cols:
        fn = 'nas_by_{0}_{1}.csv'.format(name, time)
    else:
        fn = 'nas_by_{}.csv'.format(name)
    outpath = path.join(outdir, fn)
    if replace or not path.exists(outpath):
        ### group length is the same for all
        #price_df = df.groupby(group_cols + date_group_cols).agg({'min' : {'na_ratio': nas_by_group}, 'max' : {'na_ratio': nas_by_group}, 'modal' : {'na_ratio': nas_by_group, 'len': get_group_len}})
        price_df = df.groupby(group_cols + date_group_cols).agg({'min' : {'nas': nas_by_group}, 'max' : {'nas': nas_by_group}, 'modal' : {'nas': nas_by_group, 'len': get_group_len}})
        price_df.reset_index(inplace=True)
        price_df.columns = [' '.join(col).strip() for col in price_df.columns.values]
        # Distinct commodityTonnages: do not need to subset on commodity field, since all entries are identical
        df = df.drop_duplicates(subset=['date', 'market'])
        #commodityTonnage_df = df.groupby(group_cols + date_group_cols).agg({'commodityTonnage': {'na_ratio': nas_by_group, 'len': get_group_len}})
        commodityTonnage_df = df.groupby(group_cols + date_group_cols).agg({'commodityTonnage': {'nas': nas_by_group, 'len': get_group_len}})
        commodityTonnage_df.reset_index(inplace=True)
        commodityTonnage_df.columns = [' '.join(col).strip() for col in commodityTonnage_df.columns.values]
        # Merge stat dfs on year, month index
        res_df = pd.merge(price_df, commodityTonnage_df, how="outer", on=group_cols+date_group_cols)
        res_df = df_round(res_df)
        #commodity_df = commodityTonnage_df.merge(price_df, left_index=True, right_index=True)
        commodity_df = res_df

        commodity = df['commodity'].unique()[0]
        category = df['category'].unique()[0]
        commodity_df.insert(0, 'commodity', commodity)
        commodity_df.insert(0, 'category', category)
        #cidx_df = df['year', 'month', 'commodity']
        ### NOTE: inserting NA rows
        # out_df = pd.merge(idx_df, commodity_df, how='outer', on=['year', 'month', 'commodity'])
        commodity_df.to_csv(outpath, index=False)
        res_df.to_csv(outpath, index=False)
    return

# discard varieties to have distinct commodityTonnage values
# date_group_cols = ['year', 'month']
def nas_over_time(df, commodity, date_group_cols, group_cols, outdir):
    time = '-'.join(date_group_cols)
    outpath = path.join(outdir, 'nas_by_{}.csv'.format(time))
    if replace or not path.exists(outpath):
        ### group length is the same for all
        # price_df = df.groupby(date_group_cols).agg({'min' : {'na_ratio': nas_by_group}, 'max' : {'na_ratio': nas_by_group}, 'modal' : {'na_ratio': nas_by_group, 'len': get_group_len}})
        price_df = df.groupby(group_cols + date_group_cols).agg({'min' : {'nas': nas_by_group}, 'max' : {'nas': nas_by_group}, 'modal' : {'nas': nas_by_group, 'len': get_group_len}})
        price_df.reset_index(inplace=True)
        price_df.columns = [' '.join(col).strip() for col in price_df.columns.values]
        # Distinct commodityTonnages: do not need to subset on commodity field, since all entries are identical
        df = df.drop_duplicates(subset=['date', 'market'])
        #commodityTonnage_df = df.groupby(date_group_cols).agg({'commodityTonnage': {'na_ratio': nas_by_group, 'len': get_group_len}})
        commodityTonnage_df = df.groupby(group_cols + date_group_cols).agg({'commodityTonnage': {'nas': nas_by_group, 'len': get_group_len}})
        commodityTonnage_df.reset_index(inplace=True)
        commodityTonnage_df.columns = [' '.join(col).strip() for col in commodityTonnage_df.columns.values]
        # Merge stat dfs on year, month index
        res_df = pd.merge(price_df, commodityTonnage_df, how="outer", on=date_group_cols)
        res_df = df_round(res_df)
        #commodity_df = commodityTonnage_df.merge(price_df, left_index=True, right_index=True)
        commodity_df = res_df
        commodity = df['commodity'].unique()[0]
        category = df['category'].unique()[0]
        commodity_df.insert(0, 'commodity', commodity)
        commodity_df.insert(0, 'category', category)
        #cidx_df = df['year', 'month', 'commodity']
        ### NOTE: inserting NA rows
        # out_df = pd.merge(idx_df, commodity_df, how='outer', on=['year', 'month', 'commodity'])
        res_df.to_csv(outpath, index=False)
        commodity_df.to_csv(outpath, index=False)
    return

"""
- may be computed after estimation completed
def commodityTonnages_per_variety(d, outdir):
    d = bz.by(d.variety, commodityTonnage=d.commodityTonnage.sum())
    outpath = path.join(outdir, 'commodityTonnages_per_variety.csv')
    save(d, outpath, replace)
    return

def commodityTonnages_per_variety_over_time(d, outdir):
    d = bz.by(bz.merge(d.year, d.month, d.variety), commodityTonnage=d.commodityTonnage.sum())
    ### TODO: fill year, month gaps for plotting
    outpath = path.join(outdir, 'commodityTonnages_per_variety_over_time.csv')
    save(d, outpath, replace)
    return
### necessary?
"""

### TODO: nas by region

def commodityTonnage_over_time(d, outdir, level=None):
    df = odo.odo(d, pd.DataFrame)
    d = bz.Data(df, d.dshape)
    content = False
    dr = None
    if level:
        if isinstance(level, list):
            expr = [l for l in level]
        else:
            expr = level
        name = '-'.join(level)
        outpath = path.join(outdir, 'tonnage_by_{}_year-month.csv'.format(name))
        if replace or not path.exists(outpath):
            dr = bz.by(bz.merge(d[expr], d.year, d.month, d.commodity, d.category), commodityTonnage=d.commodityTonnage.sum())
            content = True
            save(dr, outpath, replace)
    else:
        outpath = path.join(outdir, 'tonnage_by_year-month.csv')
        if replace or not path.exists(outpath):
            dr = bz.by(bz.merge(d.year, d.month, d.commodity, d.category), commodityTonnage=d.commodityTonnage.sum())
            content = True
            save(dr, outpath, replace)
    return (content, dr)

def commodityTonnage_by_month(d, outdir, level=None):
    df = odo.odo(d, pd.DataFrame)
    d = bz.Data(df, d.dshape)
    if level:
        if isinstance(level, list):
            expr = [l for l in level]
        else:
            expr = level
        name = '-'.join(level)
        outpath = path.join(outdir, 'tonnage_by_{}_month.csv'.format(name))
        if replace or not path.exists(outpath):
            do = bz.by(bz.merge(d[expr], d.month, d.commodity, d.category), commodityTonnage=d.commodityTonnage.mean())
            save(do, outpath, replace)
    else:
        outpath = path.join(outdir, 'tonnage_by_month.csv')
        if replace or not path.exists(outpath):
            do = bz.by(bz.merge(d.month, d.commodity, d.category), commodityTonnage=d.commodityTonnage.mean())
            save(do, outpath, replace)
    return

def commodityTonnage_by_year(d, outdir, level=None):
    df = odo.odo(d, pd.DataFrame)
    d = bz.Data(df, d.dshape)
    if level:
        if isinstance(level, list):
            expr = [l for l in level]
        else:
            expr = level
        name = '-'.join(level)
        outpath = path.join(outdir, 'tonnage_by_{}_year.csv'.format(name))
        if replace or not path.exists(outpath):
            do = bz.by(bz.merge(d[expr], d.year, d.commodity, d.category), commodityTonnage=d.commodityTonnage.sum())
            save(do, outpath, replace)
    else:
        outpath = path.join(outdir, 'tonnage_by_year.csv')
        if replace or not path.exists(outpath):
            do = bz.by(bz.merge(d.year, d.commodity, d.category), commodityTonnage=d.commodityTonnage.sum())
            save(do, outpath, replace)
    return

def commodityTonnage_by_level(d, outdir, level):
    name = '-'.join(level)
    outpath = path.join(outdir, 'tonnage_by_{}.csv'.format(name))
    if replace or not path.exists(outpath):
        dr = bz.by(bz.merge(d[level], d.commodity, d.year, d.month, d.category), commodityTonnage=d.commodityTonnage.sum())
        do = bz.by(bz.merge(d[level], d.commodity, d.category), commodityTonnage=d.commodityTonnage.sum())
        #d[[level, 'commodity']]
        save(do, outpath, replace)
        content, by_year_month = commodityTonnage_over_time(dr, outdir, level)
        commodityTonnage_by_year(dr, outdir, level)
        if content:
            commodityTonnage_by_month(by_year_month, outdir, level)
    return

### need additional group by and sum after appending has produced figures for all markets
"""
def commodityTonnage_by_market(d, outdir):
    outpath = path.join(outdir, 'tonnage_by_market.csv')
    if replace or not path.exists(outpath):
        do = bz.by(bz.merge(d.market, d.commodity), commodityTonnage=d.commodityTonnage.sum())
        save(do, outpath, replace)
    #by_year_month = commodityTonnage_over_time(do, outdir)
    #commodityTonnage_by_time_unit(do, outdir, 'year')
    #commodityTonnage_by_time_unit(by_year_month, outdir, 'month')
    return

def commodityTonnage_by_district(d, outdir):
    outpath = path.join(outdir, 'tonnage_by_district.csv')
    if replace or not path.exists(outpath):
        do = bz.by(bz.merge(d.market, d.commodity), commodityTonnage=d.commodityTonnage.sum())
        save(do, outpath, replace)
    return

def commodityTonnage_by_state(d, outdir):
    outpath = path.join(outdir, 'tonnage_by_state.csv')
    if replace or not path.exists(outpath):
        do = bz.by(bz.merge(d.market, d.state), commodityTonnage=d.commodityTonnage.sum())
        save(do, outpath, replace)
    return
"""

# data coverage global by commodity
# data coverage local(market) by commodity

# http://stackoverflow.com/questions/15322632/python-pandas-df-groupby-agg-column-reference-in-agg
def get_group_coverage(df, date_range=False, loc=True):
    # can I do != np.nan?
    #df = df[(df['commodityTonnage'] != '-') | (df['min'] != '-') | (df['max'] != '-') | (df['modal'] != '-')]
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
    coverage = pd.Series({'coverage' : count})#, '#dates' : count}) # index=[df[group_col].unique()[0]])
    return coverage

### WTF IS DIFFERENCT BETWEEN GET_COVERAGE CODE AND GET_LOC_COVERAGE?
def get_coverage(df, date_range, outdir, group_cols = ['commodity']):
    name = '-'.join(group_cols)
    outpath = path.join(outdir, 'coverage_by_{}.csv'.format(name))
    if replace or not path.exists(outpath):
        res_df = None
        if group_cols:
            grouped = df.groupby(group_cols)
            res_df = grouped.apply(lambda x: get_group_coverage(x, date_range, False))
        else:
            dates = df['date'].unique()
            commodity_cover = len(dates)/len(date_range)
            res_df = pd.DataFrame([[commodity_cover]], columns=['coverage'])
            group_cols = ['commodity']
        #if res_df.index.name:
        res_df.reset_index(inplace=True)
        commodity = df['commodity'].unique()[0]
        if not 'commodity' in res_df.columns:
            commodity = df['commodity'].unique()[0]
            category = df['category'].unique()[0]
            res_df.insert(0, 'commodity', commodity)
            res_df.insert(0, 'category', category)
        save(res_df, outpath, replace)
    return

def get_loc_coverage(df, subset_cols, date_group_cols, group_cols, date_range, outdir, size_col=None):
    name = '-'.join(group_cols)
    time = '-'.join(date_group_cols)
    if date_group_cols:
        outpath = path.join(outdir, 'coverage_by_{0}_{1}.csv'.format(name, time))
    else:
        outpath = path.join(outdir, 'coverage_by_{}.csv'.format(name))
    if replace or not path.exists(outpath):
        distinct_df = df.drop_duplicates(subset=subset_cols)
        grouped = distinct_df.groupby(date_group_cols + group_cols)
        res = grouped.apply(lambda x: get_group_coverage(x, date_range)) #aggregate(lambda col: get_coverage(x, date_range)).date)
        ### WHY NOT SIMPLY RETURN THE NUMBER OF UNIQUE DATES IN THE GROUP
        ### the following code (unique entries by market (size_col)) seems to be unnecessary since dates will already be unique by state or district
        """
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
        """
        res.reset_index(inplace=True)
        commodity = df['commodity'].unique()[0]
        category = df['category'].unique()[0]
        res.insert(0, 'commodity', commodity)
        res.insert(0, 'category', category)
        ### now set reset_index and add commodity column
        save(res, outpath, replace)
        if len(date_group_cols + group_cols) > 2:
            return res
    return

def mean_by_month(df, outdir, group_cols):
    name = '-'.join(group_cols)
    outpath = path.join(outdir, 'coverage_by_{}_month.csv'.format(name))
    if replace or not path.exists(outpath):
        ### TODO: include year range
        df.drop('year', axis=1, inplace=True)
        df = df.groupby(['month'] + group_cols).mean()
        df.reset_index(inplace=True)
        save(df, outpath, replace)
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
    #get_loc_coverage(df, market_cols, [], ['state', 'district', 'market'], date_range, outdir)
    get_loc_coverage(df, ['date', 'district'], [], ['state', 'district'], date_range, outdir, 'market')
    get_loc_coverage(df, ['date', 'state'], [], ['state'], date_range, outdir, 'market')

    #get_loc_coverage(df, market_cols, ['year'], ['state', 'district', 'market'], None, outdir)
    get_loc_coverage(df, ['date', 'district'], ['year'], ['state', 'district'], None, outdir, 'market')
    get_loc_coverage(df, ['date', 'state'], ['year'], ['state'], None, outdir, 'market')

    #ym_market = get_loc_coverage(df, market_cols, ['year', 'month'], ['state', 'district', 'market'], None, outdir)
    ym_district = get_loc_coverage(df, ['date', 'district'], ['year', 'month'], ['state', 'district'], None, outdir, 'market')
    ym_state = get_loc_coverage(df, ['date', 'state'], ['year', 'month'], ['state'], None, outdir, 'market')
    # on date X 7 distinct commodityTonnage entries, 12 distinct price entries for a commodity
    #(comm_cover, year_comm_cover, year_month_comm_cover), (year_market_cover, year_district_cover, year_state_cover)

    ### TODO: by month mean coverage
    ### additional group by month: and call avg
    #mean_by_month(ym_market, outdir, ['state', 'district', 'market'])
    mean_by_month(ym_district, outdir, ['state', 'district'])
    mean_by_month(ym_state, outdir, ['state'])
    return


### TODO: compute some basic intro statistics for the analysis part of the thesis here
# --> more complicated stuff may follow with R?
def compute_stats(data_dir, filename, category, commodity):
    ### PROBLEM: some commodities are still spread over multiple files => solve by gathering stats over multiple files
    csvr = bz.resource(filename)
    ds = bz.discover(csvr)
    d = bz.Data(filename, dshape=ds)
    category = list(d.category)[0]
    #commodity = list(d.commodity)[0]
    d = bz.transform(d, year=d.date.year, month=d.date.month)
    outdir = path.join(data_dir, 'stats', category, commodity)
    if not path.isdir(outdir):
        os.makedirs(outdir)

    nas_by_commodity(d, commodity, outdir)

    ### NOTE: transform: add date and month column
    # problem here is that it's not simply calling a predefined function
    ### NEED PANDAS FOR THIS
    #bz.by(bz.merge(d.year, d.month), commodityTonnage=d.commodityTonnage.sum())
    start = time.time()
    df = odo.odo(d, pd.DataFrame)
    elapsed = time.time() - start
    print('{0} took {1} secs to be loaded into pandas df with odo'.format(commodity, np.round(elapsed, 2)))

    # TODO: read the latest date from config/or use yesterdays date
    date_range = pd.date_range('1/1/2002', time.strftime('%m/%d/%Y'))
    #df = fill_records(df, date_range)

    ### In what particular periods do NAs occur?
    # does data quality improve over time? na ratio per (year, month) => further group by commodity before saving final dataframe
    get_loc_nas(df, commodity, [], ['state'], outdir)
    get_loc_nas(df, commodity, [], ['state', 'district'], outdir)
    #get_loc_nas(df, commodity, [], ['state', 'district', 'market'], outdir)

    get_loc_nas(df, commodity, ['year', 'month'], ['state'], outdir)
    get_loc_nas(df, commodity, ['year', 'month'], ['state', 'district'], outdir)
    #get_loc_nas(df, commodity, ['year', 'month'], ['state', 'district', 'market'], outdir)

    get_loc_nas(df, commodity, ['year'], ['state'], outdir)
    get_loc_nas(df, commodity, ['year'], ['state', 'district'], outdir)
    #get_loc_nas(df, commodity, ['year'], ['state', 'district', 'market'], outdir)

    get_loc_nas(df, commodity, ['month'], ['state'], outdir)
    get_loc_nas(df, commodity, ['month'], ['state', 'district'], outdir)
    #get_loc_nas(df, commodity, ['month'], ['state', 'district', 'market'], outdir)

    nas_over_time(df, commodity, ['year', 'month'], [], outdir)
    nas_over_time(df, commodity, ['year'], [], outdir)
    nas_over_time(df, commodity, ['month'], [], outdir)

    ### NOTE: taking into account that commodityTonnages are repeated:
    commodityTonnages = d[['date', 'state', 'district', 'market', 'category', 'commodity', 'commodityTonnage', 'year', 'month']].distinct()
    content, commodityTonnage_by_year_month = commodityTonnage_over_time(commodityTonnages, outdir)
    commodityTonnage_by_year(commodityTonnages, outdir)
    if content:
        commodityTonnage_by_month(commodityTonnage_by_year_month, outdir)

    #commodityTonnage_by_level(commodityTonnages, outdir, ['state', 'district', 'market'])
    commodityTonnage_by_level(commodityTonnages, outdir, ['state', 'district'])
    commodityTonnage_by_level(commodityTonnages, outdir, ['state'])
    """
    commodityTonnage_by_month(commodityTonnage_by_year_month, outdir)
    commodityTonnage_by_year(commodityTonnages, outdir)
    # commodityTonnage by state,( district,) market
    commodityTonnage_by_market(commodityTonnages, outdir)
    commodityTonnage_by_district(commodityTonnages, outdir)
    commodityTonnage_by_state(commodityTonnages, outdir)
    """

    get_coverages(df, date_range, outdir, commodity)
    ### TODO:
    # display some of these statistics in the application
    """
    make nice printout
    to compute commodityTonnage stats by commodity, first have to drop variety column and then remove all duplicates
    df.drop_duplicates() --> df.distinct()
    trick to work with blaze: fill unwanted columns (variety) with unique string/float and call drop duplicates on dataframe

    Questions per commodity (can be answered with single files):
        What is the variety with the most commodityTonnages per commodity? => commodityTonnages per variety
        What markets have the most commodityTonnages per commodity?
        What months have the most commodityTonnages per commodity?
        What weekdays have the most commodityTonnages per commodity?
        Average price per commodity?
            - by year
            - by month (seasonal phenomena?)
            - by year, month
        TODO:
        What is the NA ratio of commodityTonnages? Are there any particular patterns to this ratio?
            - by month? => DONE
            - by market? --> COULD USE THIS TO VISUALIZE DATA QUALITY!
            - by state?

    Questions that can't be answered with single files => use pymongo or odo to load multiple collections into dataframe?
    - commodityTonnage tonnage by commodity by(commodity, ==> commodityTonnage.sum()) (total)
    - commodityTonnage tonnage by category?
    - avg price by commodity by(commodity, ==> modal.mean())
    - minimum modal price by commodity
    - maximum modal price by commodity
    - commodityTonnage tonnages by years (total)
    - commodityTonnage tonnages by months (accumulative)
    - commodityTonnage tonnages by commodity,years (total)
    - commodityTonnage tonnages by commodity, month (accumulative)
    - commodityTonnage tonnages by commodity, month, year

    TODO: test all of these in ipython first
    by(df.commodity, total_commodityTonnages = df.commodityTonnage.sum())

    - NA percentage for tonnages of commodity

    - NA percentage for tonnages of commodity
    """
    return

def get_groupcols(filename):
    splits = filename.rstrip('.csv').split('_')[-2:]
    if 'by' in splits:
        splits = splits[1:]
    to_flatten = list(map(lambda x: x.split('-'), splits))
    group_cols = [item for sublist in to_flatten for item in sublist]
    return group_cols

def wavg(arr1, arr2):
    return (arr1 * arr2).sum() / arr2.sum()
### TODO: how to aggregate coverage?
def group_wavg(group):
    c = group['coverage']
    w = group['records']
    return wavg(c, w)

def name_cols(cols):
    del cols[-2:]
    cols = cols + ['coverage', '#dates']
    return cols

def aggregate_tonnage(all_dir, stacked_files, headers):
    files = filter(lambda x: 'tonnage' in x, stacked_files)
    ### -
    for filename in files:
        #df = pd.DataFrame.from_csv(filename, index_col=None)
        #df['commodityTonnage'] = df['commodityTonnage'].astype('float64')
        df = pd.read_csv(filename, index_col=None, dtype='object') # preempt pandas from inferring types
         #df['commodityTonnage'] = df['commodityTonnage'].astype('float64')
        # remove superfluous headers from dfs
        df = df[-df.isin(headers).any(axis=1)]
        df = df.reindex()
        df.to_csv(filename, index=False)
        df = pd.DataFrame.from_csv(filename, index_col=None)
        df = df_round(df)
        group_cols = get_groupcols(filename)
        if 'commodity' in df.columns:
            grouped = df.groupby(group_cols + ['commodity', 'category']).commodityTonnage.sum()
            grouped = grouped.reset_index()
            outpath = path.join(all_dir, 'commodity_'+filename)
            grouped = df_round(grouped)
            grouped.to_csv(outpath, index=False)

            df = df.drop('commodity', axis=1)

        grouped = df.groupby(group_cols).commodityTonnage.sum()
        grouped = grouped.reset_index()
        outpath = path.join(all_dir, 'total_'+filename)
        grouped = df_round(grouped)
        grouped.to_csv(outpath, index=False)
    return

"""
def nas_price_wavg(group):
    min_ratio = group['min na_ratio']
    max_ratio = group['max na_ratio']
    price_len = group['modal len']
    modal_ratio = group['modal na_ratio']
    min_ratio = wavg(min_ratio, price_len)
    max_ratio = wavg(max_ratio, price_len)
    modal_ratio = wavg(modal_ratio, price_len)
    price_len = price_len.sum()

    return pd.Series([min_ratio, max_ratio, modal_ratio, price_len], index=['min na_ratio', 'max na_ratio', 'modal na_ratio', 'modal len'])

def nas_commodityTonnage_wavg(group):
    commodityTonnage_ratio = group['commodityTonnage na_ratio']
    commodityTonnage_len = group['commodityTonnage len']
    commodityTonnage_ratio = wavg(commodityTonnage_ratio, commodityTonnage_len)
    commodityTonnage_len = commodityTonnage_len.sum()

    return pd.Series([commodityTonnage_ratio, commodityTonnage_len], index=['commodityTonnage na_ratio', 'commodityTonnage len'])
"""

### TODO: isn't this bullshit?
### doesn't it just suffice to count the total number of NAs?
def nas_price_wavg(group):
    min_nas = group['min nas']
    max_nas = group['max nas']
    price_nas = group['modal nas']
    price_len = group['modal len']
    min_nas = wavg(min_nas, price_len)
    max_nas = wavg(max_nas, price_len)
    price_nas = wavg(price_nas, price_len)
    price_len = price_len.sum()
    return pd.Series([min_nas, max_nas, price_nas, price_len], index=['min nas', 'max nas', 'modal nas', 'modal len'])

def nas_commodityTonnage_wavg(group):
    commodityTonnage_nas = group['commodityTonnage nas']
    commodityTonnage_len = group['commodityTonnage len']
    commodityTonnage_nas = wavg(commodityTonnage_nas, commodityTonnage_len)
    commodityTonnage_len = commodityTonnage_len.sum()
    return pd.Series([commodityTonnage_nas, commodityTonnage_len], index=['commodityTonnage nas', 'commodityTonnage len'])

def aggregate_nas(all_dir, stacked_files, headers):
    files = filter(lambda x: 'nas' in  x, stacked_files)
    for filename in files:
        df = pd.read_csv(filename, index_col=None, dtype='object')
        # remove superfluous headers from dfs
        df = df[-df.isin(headers).any(axis=1)]
        df = df.reindex()
        df.to_csv(filename, index=False)
        df = pd.DataFrame.from_csv(filename, index_col=None)
        df = df_round(df)
        group_cols = get_groupcols(filename)
        if group_cols == ['commodity']:
            continue
        if 'commodity' in df.columns:
            ### TODO: weighted average
            # price_df = df.groupby(group_cols).apply(nas_price_wavg)
            price_df = df.groupby(group_cols).agg({'min nas' : np.sum, 'max nas' : np.sum, 'modal nas' : np.sum, 'modal len' : np.sum})
            price_df.reset_index(inplace=True)
            #commodityTonnage_df = df.groupby(group_cols).apply(nas_commodityTonnage_wavg)
            commodityTonnage_df = df.groupby(group_cols).agg({'commodityTonnage nas' : np.sum, 'commodityTonnage len' : np.sum})
            commodityTonnage_df.reset_index(inplace=True)
            # Merge stat dfs on year, month index
            res_df = pd.merge(price_df, commodityTonnage_df, how="outer", on=group_cols)
            res_df = df_round(res_df)
            outpath = path.join(all_dir, 'commodity_'+filename)
            res_df.to_csv(outpath, index=False)

            df = df.drop('commodity', axis=1)

        # price_df = df.groupby(group_cols).apply(nas_price_wavg)
        price_df = df.groupby(group_cols).agg({'min nas' : np.sum, 'max nas' : np.sum, 'modal nas' : np.sum, 'modal len' : np.sum})
        price_df.reset_index(inplace=True)
        # commodityTonnage_df = df.groupby(group_cols).apply(nas_commodityTonnage_wavg)
        commodityTonnage_df = df.groupby(group_cols).agg({'commodityTonnage nas' : np.sum, 'commodityTonnage len' : np.sum})
        commodityTonnage_df.reset_index(inplace=True)
        # Merge stat dfs on year, month index
        res_df = pd.merge(price_df, commodityTonnage_df, how="outer", on=group_cols)
        res_df = df_round(res_df)
        outpath = path.join(all_dir, 'total_'+filename)
        res_df.to_csv(outpath, index=False)
    return

### TODO: does this even make sense???
def aggregate_coverage(all_dir, stacked_files, headers):
    files = filter(lambda x: 'coverage' in  x, stacked_files)
    for filename in files:
        print(filename)
        df = pd.read_csv(filename, index_col=None, dtype='object')
        df = df[-df.isin(headers).any(axis=1)]
        df = df.reindex()
        df.to_csv(filename, index=False)
        df = pd.DataFrame.from_csv(filename, index_col=None)
        df = df_round(df)
        group_cols = get_groupcols(filename)
        if group_cols == ['commodity']:
            continue
        if 'commodity' in df.columns:
            grouped = df.groupby(group_cols + ['commodity'])
            """
            if 'records' in df.columns:
                weighted_avg = grouped.apply(group_wavg)
                records = grouped.records.sum()
                if isinstance(weighted_avg, pd.core.series.Series) or isinstance(records, pd.core.series.Series):
                    res = pd.concat([weighted_avg, records], axis=1)
                else:
                    res = weighted_avg.merge(records, left_index=True, right_index=True)
            else:
                print(grouped.coverage.mean())
                res = grouped.coverage.mean()
            """
            res = grouped.agg({'coverage': np.mean})#, '#dates': np.sum})
            res.reset_index(inplace=True)
            #res.columns = name_cols(list(res.columns))
            #print(res.head())
            #print(res.columns)
            outpath = path.join(all_dir, 'commodity_'+filename)
            #print(outpath)
            res = df_round(res)
            res.to_csv(outpath, index=False)

            df = df.drop('commodity', axis=1)
        grouped = df.groupby(group_cols)
        """
        if 'records' in df.columns:
            weighted_avg = grouped.apply(group_wavg)
            records = grouped.records.sum()
            if isinstance(weighted_avg, pd.core.series.Series) or isinstance(records, pd.core.series.Series):
                res = pd.concat([weighted_avg, records], axis=1)
            else:
                res = weighted_avg.merge(records, left_index=True, right_index=True)
        else:
            res = grouped.coverage.mean()
        """
        res = grouped.agg({'coverage': np.mean})#, '#dates': np.sum})
        res.reset_index(inplace=True)
        #res.columns = name_cols(list(res.columns))
        #print(res.head())
        #print(res.columns)
        outpath = path.join(all_dir, 'total_'+filename)
        #print(outpath)
        res = df_round(res)
        res.to_csv(outpath, index=False)
    return

def aggregate_stacked(data_dir):
    headers = ['commodity', 'year', 'month', 'state', 'district', 'market', 'commodityTonnage', 'records', 'coverage']
    stacked_dir = path.join(data_dir, 'stats', 'all', 'stacked')
    os.chdir(stacked_dir)
    all_dir = path.join(data_dir, 'stats', 'all')
    stacked_files = glob.glob('*.csv')
    aggregate_tonnage(all_dir, stacked_files, headers)
    aggregate_coverage(all_dir, stacked_files, headers)
    aggregate_nas(all_dir, stacked_files, headers)
    ### TODO: weighted average of coverages using records

    ### HOW TO aggregate coverage?
    return


def combine_commodity_stats(data_dir):
    basedir = path.join(data_dir, 'stats')
    all_dir = path.join(basedir, 'all')
    if path.isdir(all_dir):
        shutil.rmtree(all_dir)
    os.makedirs(all_dir)
    categories = os.listdir(basedir)
    categories.remove('all')
    for category in categories:
        if path.isfile(category) or category == 'plots':
            continue
        catdir = path.join(basedir, category)
        os.chdir(catdir)
        folders = os.listdir(catdir)
        for folder in folders:
            print(category, folder)
            folder_dir = path.join(catdir, folder)
            os.chdir(folder_dir)
            files = glob.glob('*.csv')
            for filename in files:
                ### TODO: check to see if this works
                if 'variety' in filename:
                    continue
                outdir = path.join(all_dir, 'stacked')
                if not path.isdir(outdir):
                    os.makedirs(outdir)
                outpath = path.join(outdir, filename)
                cmd = '/bin/bash -c \"cat {0} >> {1}\"'.format(filename, outpath, replace)
                print(cmd)
                os.system(cmd)
        os.chdir(all_dir)
    # delete *all.csv files
    # for every category, commodity read files
    # and execute bash append command
    return

def get_file_commodity(filename):
    start = time.time()
    d = bz.Data(filename)
    ### TODO: test this! how to quickly get first element of array
    commodity = list(d['commodityTranslated'])[0]
    elapsed = time.time() - start
    print('{0} took {1} secs to be read with blaze'.format(commodity, np.round(elapsed, 2)))
    category = list(d['category'])[0]
    return (category, commodity)

def main(overwrite=True):
    global replace
    replace=ast.literal_eval(overwrite)
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
    selected_commodities = json.load(open(path.join(data_dir, 'commodities', 'selected_commodities.json')))
    print(selected_commodities)
    for folder in folders:
        if path.isfile(folder):
            continue
        if not folder in selected_commodities:
            continue
        if folder != 'Cereals':
            continue
        print('Switching to category {}'.format(folder))
        os.chdir(path.join(folder, 'integrated'))
        files = glob.glob('*.csv')
        for filename in files:
            category, commodity = get_file_commodity(filename)
            print(selected_commodities[folder])
            if not commodity in selected_commodities[folder]:
                print('Commodity {0} from category {1} was not selected! Skipping..'.format(commodity, folder))
                continue
            print('Computing stats for {}'.format(filename))
            compute_stats(data_dir, filename, category, commodity)
            ### TODO: write unpack and print function!
        os.chdir(src_dir)
    os.chdir(init_dir)

    combine_commodity_stats(data_dir)
    aggregate_stacked(data_dir)
    return

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(*sys.argv[1:])
    else:
        print('usage: {} [replace=True|False]'.format(sys.argv[0]))
