import os
from os import path
import glob
import numpy as np
import pandas as pd
import matplotlib as mpl
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import seaborn as sns

def ratio_to_num(df):
    df['nas'] = df['na_ratio'] * df['records']
    df.drop('na_ratio', inplace=True)
    return df

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

### TODO: add column to be plotted on y axis?
def page_barplots(title, page_id, chunk, pdf):
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
        sns.barplot(x="month", y="arrival", data=group, ax=ax)
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
    # save to pdf with custom save function
    return

def custom_bar_plot():
    return

def custom_line_plot():
    return

### TODO: save as png and pdf?
def simple_barplot(df, filename):
    ## how to have multiple y values? pass list?
    barplot = sns.barplot(x="month", y="arrival", data=df)
    ### TODO: check how to save this figure 
    outpath = 'some/out.loc'
    barplot.savefig(outpath)
    return

### TODO: pass meta information: meta=[type, category]
def longitudinal_plot(df, group_cols, df_cat, ext='pdf'):
    #df['date'] = df.apply(lambda row: str(int(row['year'])) + '-' + str(int(row['month'])), axis=1)
    #df = pd.DataFrame.from_csv(filename, index_col = None)
    ### BAR PLOT HAS AS X-AXIS THE TIME COL THAT WAS NOT GROUPED BY
    grouped = df.groupby(group_cols)
    plot_chunks = chunks(list(grouped), 6)
    print(plot_chunks)
    ### TODO: decide on savepath
    ### TODO: when plotting by commodity, the specific commodity has to be appended to filename
    ### what about directory structure?

    fig_name = filename.replace('csv', ext)
    title = ' '.join(filename.strip('.csv').split('_'))
    page_id = 1
    # need group by for those files that contain level
    with sns.axes_style('ticks'):
        with PdfPages(fig_name) as pdf:
            for chunk in plot_chunks:
                page_barplots(title, page_id, chunk, pdf)
                page_id+=1
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

### HAVE TO PASS AGGREGATION LVL TO DECIDE IF SINGLE OR LONGITUDINAL PLOT?
### TODO: pass filename all the way?
def plot_by_time(time_cols, filename, df=pd.DataFrame()):
    col_len = len(time_cols)
    if filename:
        df = pd.DataFrame.from_csv(filename, index_col = None)
    if df.empty:
        print('No commodity group given to plot! Exiting..\n')
        print(time_cols)
        return
    if col_len== 2:
        print('Preparing longitudinal plot for:\n', time_cols)
        longitudinal_plot(df, ['year'])
    elif col_len == 1:
        single_bar_plot(df)
        ### TODO: longitudinal plot vs other options: pdf of totals for all states, all districts in a state, pdf of single plots for commodities in a category
        #single_barplot(filename) if aggr_lvl == 'total' else longitudinal_plot(filename, time_cols, aggr_lvl)
        ## plot all commodities together per commodity aggr_lvl
    else:
        print('Something is fishy here:\n', time_cols)
    return

### NOTE: not exclusive to total (actually plot_by_lvl), is warapped in plot_commodity_by_lvl to plot commodity group dfs
### a.k.a plot_TOTAL_by_lvl
def plot_by_lvl(admin_lvl, time_cols, filename, df=pd.DataFrame()):
    ### NOTE: sure this is a longitudinal plot with level as group_col???
    if filename:
        df = pd.DataFrame.from_csv(filename, index_col = None)
    if df.empty:
        print('No commodity group given to plot! Exiting..\n')
        print(admin_lvl, time_cols)
        return
    if len(time_cols) > 1:
        ### one longitudinal plot by adminstration level group
        ### month on x-axis, one subplot of state or [state, district] per year
        grouped = df.groupby(admin_lvl)
        for admin_unit, group in grouped:
            print('Preparing longitudinal plot for:\n', admin_unit, 'years')
            longitudinal_plot(group, ['year']) 
    elif len(admin_lvl) > 1:
        ## month or year => x-axis, one subplot by district
        # first group by state and then group by district in longitudincal plot
        grouped = df.groupby('state')
        for state, group in grouped: 
            print('Preparing longitudinal plot for:\n', state, 'districts')
            longitudinal_plot(group, ['district'])
    else: ### case (state, month|year)
    ### pass admin_lvl as meta info
    ### one longitudinal plot for all admin level groups
        print('Preparing longitudinal plot for:\n', 'states')
        longitudinal_plot(df, ['state'])
    return 

def plot_by_commodity(admin_lvl, time_cols, filename):
    ### TODO: CREATE OUTFILE NAME HERE AND PASS ALONG
    df = pd.DataFrame.from_csv(filename, index_col = None)
    if len(time_cols) > 1:
        commodity_grouped = df.groupby('commodity')
        for comm, comm_group in commodity_grouped:
            ### TODO: need to pass group_col?
            if admin_lvl:
                plot_by_lvl(admin_lvl, time_cols, None, comm_group) 
            else:
                plot_by_time(time_cols, None, comm_group)
    else:
        category_grouped = df.groupby('category')
        ### TODO: not all cases dealt with: if 'year' or 'month' pdf per cat plot => means group by category (need to add category columms to stats data)
        # group_col for longitudinal plot: commodity
        for cat, cat_group in category_grouped:
            # how to pass commodity group to these functions
    return

def get_cols_from_fn(filename):
    splits = filename.rstrip('.csv').split('_')[-2:]
    to_flatten = list(map(lambda x: x.split('-'), splits))
    # return (admin_level_cols, time_cols)
    if 'by' in splits:
        return (to_flatten[0], to_flatten[1])
    else:
        return (None, to_flatten[0])

### TODO: need inverse dictionary for commodity category lookup        
def plot_aggr_lvl_handler(files, aggr_lvl, type):
    for filename in files:
        admin_lvl_cols, time_cols = get_cols_from_fn(filename) 
        ### TODO; if 'commodity' aggr_lvl simply pass this variable to extend code in plot wrapper (the plot function does not care what it's plotting)
        if admin_lvl_cols:
            ### TODO: longitudinal plot vs other options: pdf of totals for all states, all districts in a state
            if aggr_lvl == 'total':
                plot_by_lvl(admin_lvl_cols, time_cols, filename)
            elif aggr_lvl == 'commodity':
                ### there are commodity 'total' files? no
                ### ==> NOTE: there is always a group col
                # difference between 'total' and 'commodity' files is simply additional groupby by commodity
                plot_by_commodity(admin_lvl_cols, time_cols, filename)
            else:
                print('Wrong aggregation level: ', aggr_lvl)
        else:
            ### TODO: how to deal with 'total' vs 'commodity' here?
            if aggr_lvl == 'total':
                plot_by_time(time_cols, filename)
            elif aggr_lvl == 'commodity':
                plot_by_commodity(None, time_cols, filename)    
            else:
                print('Wrong aggregation level: ', aggr_lvl)
    return
    # if total: do smth
    # if commodity: df has to be loaded, grouped by commodity and plots executed by group

def get_files_by_aggr_type(ftype):
    files = glob.glob('*{}*.csv'.format(ftype))
    print('files by type {}:\n'.format(ftype), files)
    return files

def filter_files_on_aggr_lvl(files, aggr_lvl):
    files = list(filter(lambda x: aggr_lvl in x, files))
    return files

def plot_handler(ftype):
    type_files = get_files_by_aggr_type(ftype)
    ### now split by total vs commodity
    total_files = filter_files_on_aggr_lvl(type_files, 'total')
    comm_files = filter_files_on_aggr_lvl(type_files, 'commodity')

    ### pass some information of how to adopt plot according to plot_type (nas, coverage, arrivals) if at all necessary and not handled automatically
    plot_category_handler(total_files, 'total') 
    plot_category_handler(comm_files, 'commodity')

    get_cols_from_fn(filename, 'commodity')
    get_cols_from_fn(filename, 'commodity')




def main():
    data_dir = '../../data'
    os.chdir(data_dir)
    data_dir = os.getcwd()
    stats_dir = path.join(data_dir, 'stats', 'all')
    os.chdir(stats_dir)

    ### longitudinal plots
    files = glob.glob('*year-month.csv')
    for filename in files:
        ### need to differentiate between level plots, commodity (level) plots and total aggregate plots
        ### detect by columns?
        levels = ['state', 'district', 'market']
        longitudinal_plots(filename)


    return

if __name__ == "__main__":
    main()

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
