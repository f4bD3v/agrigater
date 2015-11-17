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

def longitudinal_plots(filename, ext='pdf'):
    #df['date'] = df.apply(lambda row: str(int(row['year'])) + '-' + str(int(row['month'])), axis=1)
    df = pd.DataFrame.from_csv(filename, index_col = None)
    grouped = df.groupby('year')
    plot_chunks = chunks(list(grouped), 6)
    print(plot_chunks)
    ### TODO: decide on savepath
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


def custom_bar_plot():
    return

def custom_line_plot():
    return

def plot_by_commodity(df):
    # df.groupby('commodity') 
    return

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
