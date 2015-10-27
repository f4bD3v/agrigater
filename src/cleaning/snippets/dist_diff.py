# coding: utf-8
import pickle
geo = pickle.load(open('geojson_admin_dict.p', 'rb'))
po = pickle.load(open('admin_hierarchy_dict.p', 'rb'))
print po.values().keys()
print po.values()
print po.keys()
po_dists_taluks = reduce(lambda x,y: dict(x.items()+y.items()), po.values())
po_dists = po_dists_taluks.values()
po_dists = po_dists_taluks.keys()
geo.values()
geo_dists_taluks = reduce(lambda x,y: dict(x.items()+y.items()), geo.values())
geo_dists = geo_dists_taluks.keys()
geo_dists
set(po_dists) - set(geo_dists)
get_ipython().magic(u'save dist_diff.py 1-22')
