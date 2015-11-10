import os
from os import path
import glob.glob
import numpy as np
import pandas as pd
import matplotlib as mpl
from mpl.backends.backend_pdf import PdfPages
import mpl.pyplot as plt
import seaborn as sns

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

### TODO: add column to be plotted on y axis?
def page_barplots(chunk, pdf):
    fig, axes = plt.subplots(nrows=3, ncols=2)
    fig.title()
    # use commodity, "arrival" as figure title
    plot_tuples = zip(chunk, axes.flat) 
    for (idx, group), ax in plot_tuples:
        print(idx)
        # use year as subplot title
        sns.barplot(x="month", y="arrival", data=group)
        pdf.savefig()

    # save to pdf with custom save function
    return

def longitudinal_plots(filename, ext='pdf'):
    #df['date'] = df.apply(lambda row: str(int(row['year'])) + '-' + str(int(row['month'])), axis=1)
    df = pd.DataFrame.from_csv(filename, index_col = None)
    grouped = df.groupby('year')
    plot_chunks = chunks(grouped, 6)
    ### TODO: decide on savepath
    fig_name = filename.replace('csv', ext)

    with PdfPages(fig_name) as pdf:
        for chunk in plot_chunks:
            page_barplots(chunk, pdf)

    return

""" 

    Automate population of subplots:
http://stackoverflow.com/questions/24828771/automate-the-populating-of-subplots
    Rotate labels:
http://stackoverflow.com/questions/26540035/rotate-label-text-in-seaborn-factorplot/26540821#26540821
    Plotting time series data with seaborn:
http://stackoverflow.com/questions/22795348/plotting-time-series-data-with-seaborn/22798911#22798911

"""

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
    files = glob.glob('year-month.csv')
    for filename in files:
        longitudinal_plots(filename)


    return

### for different commodities: plot coverage by year, month
### for different commodities: plot coverage by state, district, market
### for different commodities: plot coverage avg by month (state, district month)
