import sys
import os
from os import path
import glob
import numpy as np
import pandas as pd
import matplotlib as mpl
mpl.use('Agg')
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import seaborn as sns

data_dir = '../../data'
init_dir = os.getcwd()
os.chdir(data_dir)
data_dir = os.getcwd()
os.chdir(init_dir)
outpath = path.join(data_dir, 'stats', 'plots')
if not path.exists(outpath):
    os.makedirs(outpath)

def custom_bar_plot():
    return

def custom_line_plot():
    return

"""
    Automate population of subplots:
http://stackoverflow.com/questions/24828771/automate-the-populating-of-subplots
    Rotate labels:
http://stackoverflow.com/questions/26540035/rotate-label-text-in-seaborn-factorplot/26540821#26540821
    Plotting time series data with seaborn:
http://stackoverflow.com/questions/22795348/plotting-time-series-data-with-seaborn/22798911#22798911

"""
# # #
# # #

def ratio_to_num(df):
    df['nas'] = df['na_ratio'] * df['records']
    df.drop('na_ratio', inplace=True)
    return df

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

### TODO: add column to be plotted on y axis?
def page_barplots(xlabel, ylabels, stype, title, page_id, chunk, pdf):
    chunk_len = len(chunk)
    fig, axes = plt.subplots(nrows=3, ncols=2, dpi=100)
    sns.despine(fig)
    fig.set_size_inches([8.27,11.69])
    ### TODO: put info in subplot title!!!
    #fig.suptitle(title + ', page '+str(page_id))
    # use commodity, "arrival" as figure title
    axes = np.array(axes.flat)
    plot_tuples = zip(chunk, axes[0:chunk_len])
    for (idx, group), ax in plot_tuples:
        df = remove_group_idx(group)
        df = complete_df(df, xlabel)
        if len(ylabels) > 1:
            df = tidy_df(df, xlabel, ylabels, stype)
            sns.barplot(x=xlabel, y='value', hue=stype, data=df, ax=ax)
        else:
            sns.barplot(x=xlabel, y=ylabels[0], data=df, ax=ax)
        # using year as subplot title
        ax.set_title(idx)
        #ax.xaxis.set_major_locator(months)
        #ax.xaxis.set_major_formatter(monthsFmt)
        #ax.xaxis.set_minor_locator(mondays)

    ### remove unused axes
    for ax in axes[chunk_len:]:
        ax.axis('off')
    fig.tight_layout()
    pdf.savefig()
    plt.close()
    # save to pdf with custom save function
    return

def save_plot(fig, outpath, filename, ext):
    outfile = filename + ext
    fig.savefig(path.join(outpath, outfile))

def get_plot_filename(filename, props, ext):
    stype = props.pop('stype', None)
    atype = props.pop('aggrlvl', None)
    admin = props.pop('admin', [])
    subpath = ''
    folders = list(filter(None, [stype, atype])) + admin
    if folders:
        subpath = path.join(*folders)
        check = path.join(outpath, subpath)
        if not path.exists(check):
            os.makedirs(check)
    props = list(props.values()) + [admin]
    props = list(filter(None, props))
    props = [item if isinstance(item, list) else item for item in props]
    if props:
        props = ['-'.join(item) if isinstance(item, list) else item for item in props]
    else:
        return path.join(subpath, filename+'.'+ext)
    #elif len(props) > 1 or isinstance(props[0], list):
    props.insert(0, filename)
    outfile = '_'.join(props)
    outfile+=('.'+ext)
    # first: filter None from props
    return path.join(subpath, outfile)

def remove_group_idx(df):
    if df.index.name:
        idx_name = df.index.name
        df.reset_index(inplace=True)
        df.drop(idx_name, inplace=True)
    return df

def complete_df(df, xlabel):
    complete_df = pd.DataFrame()
    if xlabel == 'month':
        mdf = pd.DataFrame(pd.Series(np.arange(1,13)), columns=['month'])
        complete_df = mdf.merge(df, how='outer', on='month')
    elif xlabel == 'year':
        first_year = 2002
        last_year = 2015
        ydf = pd.DataFrame(pd.Series(np.arange(first_year,last_year+1)), columns=['year'])
        complete_df = ydf.merge(df, how='outer', on='year')
    else:
        print('Unknown xlabel', xlabel)
    return complete_df

def tidy_df(df, xlabel, ylabels, stype):
    #print('WARNING: df does not have an index name', df.head())
    tidy_df = pd.melt(df, id_vars=xlabel, value_vars=ylabels, var_name=stype)
    return tidy_df

### TODO: for Beverages month 10 has a huge column: find anomaly
def simple_barplot(xlabel, ylabels, stype, df, filename, exts):
    ## how to have multiple y values? pass list?
    fig = plt.figure()
    df = complete_df(df, xlabel)
    if len(ylabels) > 1:
        df = tidy_df(df, xlabel, ylabels, stype)
        barplot = sns.barplot(x=xlabel, y='value', hue=stype, data=df)
    else:
        barplot = sns.barplot(x=xlabel, y=ylabels[0], data=df)
    fig.add_axes(barplot)
    ### TODO: call complete_df() and tidy_df()
    for ext in exts:
        save_plot(fig, outpath, filename, '.'+ext)
    plt.close()
    return

def longitudinal_plot(df, group_cols, filename, props, ext='pdf'):
    #df['date'] = df.apply(lambda row: str(int(row['year'])) + '-' + str(int(row['month'])), axis=1)
    #df = pd.DataFrame.from_csv(filename, index_col = None)
    ### BAR PLOT HAS AS X-AXIS THE TIME COL THAT WAS NOT GROUPED BY
    grouped = df.groupby(group_cols)
    xlabel = props.pop('xlabel', None)
    ylabels = props.pop('ylabels', None)
    stype = props['stype']
    plot_chunks = list(chunks(list(grouped), 6))
    print('Number of chunks:', len(plot_chunks))
    ### TODO: append props info to filename?
    title = ' '.join(filename.split('_'))
    outfile = get_plot_filename(filename, props, ext)
    page_id = 1
    # need group by for those files that contain level
    with sns.axes_style('ticks'):
        with PdfPages(path.join(outpath, outfile)) as pdf:
            for chunk in plot_chunks:
                print('Number (=6?) of elements in chunk:', len(chunk))
                page_barplots(xlabel, ylabels, stype, title, page_id, chunk, pdf)
                page_id+=1
    return

def get_ylabels(stype):
    labels = []
    if stype == 'nas':
        labels = ['price_na%', 'tonnage_na%', 'joint_na%']
    elif stype == 'coverage':
        labels = ['coverage']
    elif stype == 'tonnage':
        labels = ['commodityTonnage']
    else:
        print('Non-existant stats type {}'.format(stype))
    return labels

def prepare_data(stype, df):
    if stype == 'nas':
        df['price_na%'] = df['modal nas']/df['modal len'] * 100
        df.drop('min nas', inplace=True, axis=1)
        df.drop('max nas', inplace=True, axis=1)
        df.drop('modal nas', inplace=True, axis=1)
        df.drop('modal len', inplace=True, axis=1)
        df['tonnage_na%'] = df['commodityTonnage nas']/df['commodityTonnage len'] * 100
        df.drop('commodityTonnage nas', inplace=True, axis=1)
        df['joint_na%'] = df['joint nas']/df['commodityTonnage len'] * 100
        df.drop('joint nas', inplace=True, axis=1)
        df.drop('commodityTonnage len', inplace=True, axis=1)
    return df

### TODO: add ylabels initialization
def plot_by_time(stype, aggr_lvl, time_cols, filename, df=pd.DataFrame(), comm=None, cat=None):
    col_len = len(time_cols)
    if df.empty:
        print('No commodity group given to plot! Reading dataframe from: {}'.format(filename))
        df = pd.DataFrame.from_csv(filename, index_col = None)
    df = prepare_data(stype, df)
    ylabels = get_ylabels(stype)
    props = {
        'stype' : stype,
        'aggrlvl' : aggr_lvl,
        'category' : cat,
        'commodity' : comm,
        'ylabels' : ylabels
    }
    filename = filename.replace('.csv', '')
    if col_len== 2:
        print('Preparing longitudinal plot for:', time_cols)
        props['xlabel'] = 'month'
        properties = dict(props)
        longitudinal_plot(df, ['year'], filename, properties)
    elif col_len == 1:
        xlabel = time_cols[0]
        simple_barplot(xlabel, ylabels, stype, df, filename, ['png', 'pdf'])
        ### TODO: longitudinal plot vs other options: pdf of totals for all states, all districts in a state, pdf of single plots for commodities in a category
        #single_barplot(filename) if aggr_lvl == 'total' else longitudinal_plot(filename, time_cols, aggr_lvl)
        ## plot all commodities together per commodity aggr_lvl
    else:
        print('Something is fishy here:\n', time_cols)
    return

### NOTE: not exclusive to total (actually plot_by_lvl), is warapped in plot_commodity_by_lvl to plot commodity group dfs
### a.k.a plot_TOTAL_by_lvl
### TODO: add y axes initialization
def plot_by_lvl(stype, aggr_lvl, admin_lvl, time_cols, filename, df=pd.DataFrame(), comm=None):
    ### NOTE: sure this is a longitudinal plot with level as group_col???
    if df.empty:
        print('No commodity group given to plot! Reading dataframe from: {}'.format(filename))
        df = pd.DataFrame.from_csv(filename, index_col = None)
    df = prepare_data(stype, df)
    ylabels = get_ylabels(stype)
    filename = filename.replace('.csv', '')
    props = {
        'stype' : stype,
        'aggrlvl' : aggr_lvl,
        'commodity' : comm,
        'ylabels' : ylabels
    }
    if not time_cols:
        ### TODO: totals by admin level: ignore for now
        return
    if len(time_cols) > 1:
        ### one longitudinal plot by adminstration level group
        ### month on xlabel, one subplot of state or [state, district] per year
        grouped = df.groupby(admin_lvl)
        for admin_unit, group in grouped:
            print('Preparing longitudinal plot for: ', admin_unit, 'years')
            if isinstance(admin_unit, tuple):
                admin_unit = list(admin_unit)
            props['admin'] = [admin_unit]
            props['xlabel'] = 'month'
            properties = dict(props)
            longitudinal_plot(group, ['year'], filename, properties)
    elif len(admin_lvl) > 1:
        ## month or year => xlabel, one subplot by district
        # first group by state and then group by district in longitudincal plot
        grouped = df.groupby('state')
        for state, group in grouped:
            print('Preparing longitudinal plot for: ', state, 'districts')
            props['admin'] = [state, 'districts']
            props['xlabel'] = time_cols[0]
            properties = dict(props)
            longitudinal_plot(group, ['district'], filename, properties)
    else: ### case (state, month|year) => admin_lvl == ['state']
    ### pass admin_lvl as props info
    ### one longitudinal plot for all admin level groups
        print('Preparing longitudinal plot for:', 'states')
        #props['admin'] = admin_lvl
        props['xlabel'] = time_cols[0]
        properties = dict(props)
        print('properties of states plot:', properties)
        longitudinal_plot(df, admin_lvl, filename, properties)
    return

def plot_by_commodity(stype, aggr_lvl, admin_lvl, time_cols, filename):
    ### TODO: CREATE OUTFILE NAME HERE AND PASS ALONG
    df = pd.DataFrame.from_csv(filename, index_col = None)
    if not time_cols:
        ### totals by admin lvl only, not dealt with
        return
    elif len(time_cols) > 1:
        commodity_grouped = df.groupby('commodity')
        for comm, comm_group in commodity_grouped:
            ### TODO: need to pass group_col?
            if admin_lvl:
                plot_by_lvl(stype, aggr_lvl, admin_lvl, time_cols, filename, comm_group, comm)
            else:
                plot_by_time(stype, aggr_lvl, time_cols, filename, comm_group, comm)
    else:
        ### TODO: not all cases dealt with: if 'year' or 'month' pdf per cat plot => means group by category (need to add category columms to stats data)
        # group_col for longitudinal plot: commodity
        ### TODO: category only if there is no admin_lvl!!!!
        if admin_lvl:
            # by state, district
            commodity_grouped = df.groupby('commodity')
            for comm, comm_group in commodity_grouped:
                print('Preparing longitudinal plot for:', admin_lvl, comm)
                plot_by_lvl(stype, aggr_lvl, admin_lvl, time_cols, filename, comm_group, comm)
        else:
            df = prepare_data(stype, df)
            category_grouped = df.groupby('category')
            filename = filename.replace('.csv', '')
            for cat, cat_group in category_grouped:
                # how to pass commodity group to these functions
                ### TODO: nonono, pass commodity as group col
                ### CALL Longitdunial plot directly?
                print('Preparing longitudinal plot for:', time_cols, cat)
                props = {
                    'stype' : stype,
                    'aggrlvl' : aggr_lvl,
                    'category' : cat,
                    'ylabels' : get_ylabels(stype),
                    'xlabel': time_cols[0]
                }
                longitudinal_plot(cat_group, ['commodity'], filename, props)
    return

def get_cols_from_fn(filename):
    splits = filename.rstrip('.csv').split('_')[-2:]
    to_flatten = list(map(lambda x: x.split('-'), splits))
    # return (admin_level_cols, time_cols)
    if 'by' in splits:
        if 'month' in to_flatten[1] or 'year' in to_flatten[1]:
            return (None, to_flatten[1])
        else:
            return (to_flatten[1], None)
    else:
        return (to_flatten[0], to_flatten[1])

### TODO: need inverse dictionary for commodity category lookup
def plot_aggr_lvl_handler(files, aggr_lvl, stype):
    #count = 3
    for filename in files:
        #if count < 1:
        #    continue
        admin_lvl_cols, time_cols = get_cols_from_fn(filename)
        ### TODO; if 'commodity' aggr_lvl simply pass this variable to extend code in plot wrapper (the plot function does not care what it's plotting)
        if admin_lvl_cols:
            ### TODO: longitudinal plot vs other options: pdf of totals for all states, all districts in a state
            if aggr_lvl == 'total':
                plot_by_lvl(stype, aggr_lvl, admin_lvl_cols, time_cols, filename)
            elif aggr_lvl == 'commodity':
                ### there are commodity 'total' files? no
                ### ==> NOTE: there is always a group col
                # difference between 'total' and 'commodity' files is simply additional groupby by commodity
                plot_by_commodity(stype, aggr_lvl, admin_lvl_cols, time_cols, filename)
            else:
                print('Wrong aggregation level: ', aggr_lvl)
        else:
            ### TODO: how to deal with 'total' vs 'commodity' here?
            if aggr_lvl == 'total':
                plot_by_time(stype, aggr_lvl, time_cols, filename)
            elif aggr_lvl == 'commodity':
                plot_by_commodity(stype, aggr_lvl, None, time_cols, filename)
            else:
                print('Wrong aggregation level: ', aggr_lvl)
        #count = count - 1
    return
    # if total: do smth
    # if commodity: df has to be loaded, grouped by commodity and plots executed by group

def get_files_by_aggr_type(ftype):
    files = glob.glob('*{}*.csv'.format(ftype))
    print('files by type {}:'.format(ftype), files)
    return files

def filter_files_on_aggr_lvl(files, aggr_lvl):
    files = list(filter(lambda x: aggr_lvl in x, files))
    print('files by by aggregation level {}:'.format(aggr_lvl), files)
    return files

def plot_type_handler(stype):
    type_files = get_files_by_aggr_type(stype)
    ### now split by total vs commodity
    total_files = filter_files_on_aggr_lvl(type_files, 'total')
    comm_files = filter_files_on_aggr_lvl(type_files, 'commodity')

    ### pass some information of how to adopt plot according to plot_type (nas, coverage, arrivals) if at all necessary and not handled automatically
    plot_aggr_lvl_handler(total_files, 'total', stype)
    plot_aggr_lvl_handler(comm_files, 'commodity', stype)
    return

def main(stype):
    #data_dir = '../../data'
    #os.chdir(data_dir)
    #data_dir = os.getcwd()
    stats_dir = path.join(data_dir, 'stats', 'all')
    os.chdir(stats_dir)

    print('Plotting {} stats'.format(stype))
    plot_type_handler(stype)
    #plot_type_handler('tonnage')
    #plot_type_handler('nas')
    #plot_type_handler('coverage')

    return

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ['tonnage', 'nas', 'coverage']:
        main(*sys.argv[1:])
    else:
        print('Usage {} (tonnage|nas|coverage)'.format(sys.argv[0]))

# plot vs multiplot

# total:
# - year => one bar plot (arrivals: one bar, nas: two bars, coverage: two plots with one bar)

# {year} => one plot
# {month} => one plot
# {commodity, year} => commodity plots for all years
# {commodity, month} =>
# {commodity, level, {month, year}} => longitudinal plot
### for different commodities: plot coverage by year, month
### for different commodities: plot coverage by state, district, market
### for different commodities: plot coverage avg by month (state, district month)
