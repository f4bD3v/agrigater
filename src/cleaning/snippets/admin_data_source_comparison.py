# coding: utf-8
import pickle
geo = pickle.load(open('geojson_admin_dict.p', 'rb'))
po = pickle.load(open('admin_hierarchy_dict.p', 'rb'))
print po.values().keys()
print po.values()
print po.keys()
po_dists_taluks = reduce(lambda x,y: x+y, po.values())
po_dists_taluks = reduce(lambda x,y: x.update(y), po.values())
po_dists_taluks = reduce(lambda x,y: dict(x.items()+y.items()), po.values())
po_dists_taluks
po_dists = po_dists_taluks.values()
po_dists
po_dists = po_dists_taluks.keys()
po_dists
geo_dict = pickle.load(open('geojson_admin_dict.p', 'rb'))
geo_dict
geo_dict.values()
geo_dists_taluks = reduce(lambda x,y: dict(x.items()+y.items()), geo_dict.values())
geo_dists = geo_dists_taluks.keys()
geo_dists
set(po_dists) - set(geo_dists)
get_ipython().magic(u'save dist_diff.py 1-22')
'Bangalore' in po_dists
'Visakhapatnam' in po_dists
'Yagdir' in po_dists
'Mumbai' in po_dists
'Ahmedabad' in po_dists
po.keys()
set(po.keys()) - set(geo.keys())
geo.keys()
get_ipython().magic(u'save admin_data_source_comparison.py 1-31')
