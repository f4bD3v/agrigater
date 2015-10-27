import pandas as pd
import pylev
from os import path
import json

"""
	issues:
	 [('daspur-i', 4), ('daspur-ii', 1)]
	 [('kharagpur', 15), ('kharagpur-i', 5)]
	 [('chandrakona-i', 4), ('chandrakona-ii', 3)
	only if not some valid name in another data base
"""

data_dir = '../../../data/admin/'

def uniqify(corpus, occ_dict, distance):
	# augment with value counts (which one to keep)
	words = []
	while corpus:
		center = corpus[0]
		related = [word for word in corpus if pylev.levenshtein(center, word) <= distance]
		tuples = [(word, occ_dict[word.title()]) for word in related]
		sorted_ts = sorted(tuples, key=lambda x: x[1], reverse=True)
		print(sorted_ts)
		winner = sorted_ts[0][0]
		print(corpus)
		for t in sorted_ts:
			print(t)
			corpus.remove(t[0])
		# keep taluk with highest number of occurrences
		# create dict by taking difference between corpae
		words.append(winner)
	return [x.title() for x in words]

df = pd.DataFrame.from_csv(path.join(data_dir, 'POs_data_cleaned.csv'), index_col = None)
grouping = df.groupby(['State', 'District'])
location_dict = {}
for keys, grp in grouping:
	state, dist = keys
	if not state in location_dict:
		location_dict[state] = {}
	# keys = (state, dist)
	taluk_count = grp['Taluk'].value_counts().to_dict()
	taluks = list(grp['Taluk'].unique())
	print(taluks)
	taluks = [x.lower() for x in taluks if str(x) != 'nan']
	print(taluks)
	# do not uniquify for the moment
	# taluks = uniqify(taluks, taluk_count, 1)
	print(taluks)
	location_dict[state][dist] = taluks
#print(location_dict)
j = json.dumps(location_dict, ensure_ascii=False)
f = open(path.join(data_dir, 'locations_serialized.json'), 'w')
f.write(j)
f.close()
"""
state_dist_grps = [gp.get_group() for gp in grouping.groups]
for grp in state_dist_grps:
"""	

