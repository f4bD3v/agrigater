import os
from os import path
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

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

plt.figure( ... , facecolor='white')

def plot_by_commodity(df):
    # df.groupby('commodity') 
    return

def main():
    data_dir = '../../data'
    os.chdir(data_dir)
    data_dir = os.getcwd()
    stats_dir = path.join(data_dir, 'stats', 'all')

    df['date'] = df.apply(lambda row: str(int(row['year'])) + '-' + str(int(row['month'])), axis=1)

    return

### for different commodities: plot coverage by year, month
### for different commodities: plot coverage by state, district, market
### for different commodities: plot coverage avg by month (state, district month)
