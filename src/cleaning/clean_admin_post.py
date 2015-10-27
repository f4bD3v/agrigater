import pandas as pd
import numpy as np
import string
import Levenshtein as edit
import ngram

# convert data from
# http://questionbox.in/india-state-district-sub-district-database-in-excel-csv-and-sql-query-format/
# to nice csv
# in order to clean taluks

df = pd.DataFrame.from_csv('../../data/admin/all_india_POs_Telangana.csv', index_col = None)
pos_df = df[list(df.columns[[1,7,8,9]])]
pos_df = pos_df.dropna(thresh=3)
pos_df = pos_df.drop_duplicates()

pos_df.rename(columns={'Taluk': 'taluk', 'Districtname': 'district', 'statename': 'state'}, inplace=True)

pos_df['state'] = pos_df['state'].apply(string.capwords)
pos_df['district'] = pos_df['district'].str.replace(r'\((Urban|Rural|East|West)\)', ' \1 ')
print pos_df['state']
pos_df['taluk'] = pos_df['taluk'].str.strip('*').str.strip()
#pos_df['taluk'] = pos_df['taluk'].str.replace(r'\((\w+)\)', '')
print pos_df['taluk']

df = pd.DataFrame.from_csv('../../data/admin/POs_data_cleaned.csv')
### ADD STRING REPLACEMENTS/CLEANING HERE ###
## abbr = ['cgh', 'mh', etc. ]
## already done
pos_df = pos_df.replace(np.nan, '*')
gb = pos_df.groupby(['district', 'state'])
gb_list = [gb.get_group(x) for x in gb.groups]

for df_frag in gb_list:
    print type(df_frag)
    df_frag = df_frag.copy(deep=True)
    print df_frag['taluk']
    taluk_names = list(df_frag['taluk'].unique())
    if '*' in taluk_names:
        taluk_names.remove('*')
    print taluk_names
    taluk_groups = []
    visited_taluks = []
    for n1 in taluk_names:
        for n2 in taluk_names:
            # group all taluk names within edit distance of 2
            if not (n1 in visited_taluks) and edit.distance(n1, n2) <= 2 and n1 != n2:
                print n1, n2
                print ngram.NGram.compare(n1, n2, N=3)
                taluk_groups.append([n1, n2])
                visited_taluks.append(n2)

    print taluk_groups
    counts = df_frag['taluk'].value_counts()
    correction_items = []
    for tg in taluk_groups:
        # disambiguation of grouped candidates (param: edit distance threshold) by voting
        counts_by_name = [(tn, counts[tn]) for tn in tg]
        # sort in-place
        counts_by_name.sort(key= lambda x: x[1], reverse=True)
        winning_tuple = counts_by_name[0]
        winner = winning_tuple[0]
        # remove winner from list and add as 2nd field in a tuple
        tg.remove(winner)
        correction_item = (tg, winner)
        correction_items.append(correction_item)
    print correction_items
    for corr_item in correction_items:
        repl_dict = {}
        repl_dict['taluk'] = {}
        for tn in corr_item[0]:
            # dict has to provide column (#nested dict)
            repl_dict['taluk'][tn] = corr_item[1]
        print repl_dict
        df_frag.replace(repl_dict, regex=True)
# check if df_frags are actually modified in-place
pd.concat(gb_list)
# drop duplicates that may have been created
pos_df = pos_df.drop_duplicates()

# et voila: hopefully taluks are now cleaner
