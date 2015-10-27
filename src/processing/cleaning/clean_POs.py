# coding: utf-8
import numpy as np
import pandas as pd
import re
import string
from os import path

data_dir = '../../../data'

df = pd.DataFrame.from_csv(path.join(data_dir, 'admin/all_india_POs_Telangana.csv'), index_col = None)
pos_df = df[list(df.columns[[1,7,8,9]])]
pos_df.rename(columns={'pincode' : 'PIN', 'Districtname': 'District', 'statename': 'State'}, inplace=True)
pos_df = pos_df.dropna(thresh=3)
pos_df = pos_df.drop_duplicates()
pos_df['State'] = pos_df['State'].str.strip().str.title()
pos_df['State'] = pos_df['State'].str.replace(r'\&', 'and')
pos_df['State'] = pos_df['State'].str.replace(r'Delhi$', 'NCT of Delhi')
pos_df['District'] = pos_df['District'].str.replace(r'\&', 'and')
pos_df['District'] = pos_df['District'].str.replace(r'\((KAR|HP|MH|CGH|BH)\)', '')
pos_df['District'] = pos_df['District'].str.replace(r'Bangalore$', 'Bangalore Urban')
pos_df['District'] = pos_df['District'].str.strip().str.title()
pos_df['Taluk'] = pos_df['Taluk'].str.strip('*').str.strip()
pos_df['Taluk'] = pos_df['Taluk'].str.replace(r'\.\.', '.')
pos_df['Taluk'] = pos_df['Taluk'].str.replace(r'\s+', ' ')
pos_df['Taluk'] = pos_df['Taluk'].str.replace(r'(Mandall|Mandalam|\(mdl\)|Mandal)', '', re.IGNORECASE).str.strip().str.title()
pos_df = pos_df.drop_duplicates()
pos_df = pos_df.reset_index(drop=True)
#pos_df[pd.isnull(pos_df[['PIN', 'District', 'State']]).any(axis=1)] # --> empty, there are only missing taluk values
pos_df.to_csv(path.join(data_dir, 'admin/POs_data_cleaned.csv'))
#get_ipython().magic(u'save /Users/fab/git/msthesis/data/admin/locations+POs-processing.py 1-53')
