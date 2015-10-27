# coding: utf-8
import pandas as pd
import numpy as np
from os import path

data_dir = '../../../data/production'

def reshaping(sheet_num, sheet):
	sheet= sheet.dropna(axis=0, how = 'all')
	sheet.rename(columns = dict(zip(sheet.columns, list(range(sheet.shape[1])))), inplace=True)
	col_ints = [5,6,8,9,10,11,13,14,15,18,19,20,21,25,26,28]
	sheet= sheet.drop(sheet.columns[col_ints], axis=1)
	sheet= sheet.set_index([list(range(sheet.shape[0]))])

	sheet.rename(columns={0: 'State'}, inplace=True)
	# drop column containing only 'State Name' label:
	sheet.rename(columns={2: 'Crop'}, inplace=True)
	sheet.rename(columns={4: 'District'}, inplace=True)
	sheet.rename(columns={7: 'Year'}, inplace=True)
	sheet.rename(columns={12: 'Season'}, inplace=True)
	sheet.rename(columns={17: 'Area (ha)'}, inplace=True)
	sheet.rename(columns={24: 'Production (t)'}, inplace=True)
	sheet.rename(columns={30: 'Yield (t/ha)'}, inplace=True)
	sheet.to_csv(open('column_cleaned.csv', 'w'))

	sheet.drop(1, axis=1, inplace=True)
	sheet.drop(3, axis=1, inplace=True)
	sheet.drop(16, axis=1, inplace=True)
	sheet.drop(23, axis=1, inplace=True)
	sheet.drop(29, axis=1, inplace=True)

	if sheet_num == 1:
		sheet.loc[1, 'Crop'] = np.nan
		sheet.loc[0, 'Area (ha)'] = np.nan
		sheet.loc[4, ['Year', 'Season']] = np.nan
		sheet.loc[0, 'Production (t)'] = np.nan
		sheet.loc[0, 'Yield (t/ha)'] = np.nan

	sheet= sheet.dropna(axis=0, how = 'all')
	sheet['State'] = sheet['State'].str.title()
	sheet['Crop'] = sheet['Crop'].str.title()
	sheet['District'] = sheet['District'].str.title()
	return sheet

def fillmerging(sheet):
	cols = ['State', 'Crop', 'District', 'Year', 'Season', 'Area (ha)']
	# excempt columns: [22, 'Production (t)', 27,   'Yield (t/ha)'] that need to be merged
	for col in cols:
		sheet[col] = sheet[col].fillna(method='ffill')
	# remove rows only containing state, district and crop names:
	sheet = sheet.dropna(axis=0, thresh=4)
	# fill nas of columns to be merged with zeros
	sheet = sheet.fillna(0)
	# add up columns; patterns: 0 + int or int + 0 (for total seasonal aggregates)
	sheet['Production (t)'] = sheet.apply(lambda x: x[22]+x['Production (t)'], axis=1)
	sheet.drop(22, axis=1, inplace=True)
	sheet['Yield (t/ha)'] = sheet.apply(lambda x: x[27]+x['Yield (t/ha)'], axis=1)
	sheet.drop(27, axis=1, inplace=True)
	sheet = sheet.set_index([list(range(sheet.shape[0]))])
	return sheet

xl = pd.ExcelFile(path.join(data_dir, 'district-wise_yearly_crop_prod_stats.xls'))
sheet1= xl.parse('Sheet1', index_col = None)
sheet1 = reshaping(1, sheet1)
sheet1 = fillmerging(sheet1)

sheet2= xl.parse('Sheet2', index_col = None)
sheet2 = reshaping(2, sheet2)
sheet2 = fillmerging(sheet2)

sheet3= xl.parse('Sheet3', index_col = None)
sheet3 = reshaping(3, sheet3)
sheet3 = fillmerging(sheet3)

sheet4= xl.parse('Sheet4', index_col = None)
sheet4 = reshaping(4, sheet4)
sheet4 = fillmerging(sheet4)

df = pd.concat([sheet1, sheet2, sheet3, sheet4], axis=0)
df = df.set_index([list(range(df.shape[0]))])
# remove last row, ministry sig
df.ix[:-1]
print(df.head())
print(df.tail())
df.to_csv(open(path.join(data_dir, 'production_data_formatted.csv'), 'w'))