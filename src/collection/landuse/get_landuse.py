# would have been faster to extract with lxml, but would have entailed cleaning of names
from os import path
import requests
import lxml.html
import re

item_request_headers = {
	'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36',
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
	'Host': 'lus.dacnet.nic.in',
	'Origin': 'http://lus.dacnet.nic.in',
	'Connection': 'keep-alive',
	'Upgrade-Insecure-Requests': 1,
	#'Content-Type': 'application/x-www-form-urlencoded',
	'DNT': 1,
	'Referer': 'http://lus.dacnet.nic.in/dt_lus.aspx',
	'Accept-Encoding': 'gzip, deflate',
	'Accept-Language': 'de-DE,de;q=0.8,en-US;q=0.6,en;q=0.4,fr;q=0.2,es;q=0.2'
}

data_dir = '../../../data/landuse'
url = 'http://lus.dacnet.nic.in/dt_lus.aspx'
s = requests.Session()
r = s.get(url)
# get headers sent to the server
print(r.request.headers)
print(r.cookies)

source = r.text # r.content - access response body as bytes
html = lxml.html.fromstring(source)
select = html.cssselect('select#DropDownList2')
options = select[0].getchildren()
option_vals = list(map(lambda x: x.text_content(), options))

ids = [2, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14]
buttons = {}
for id in ids:
	button_id = 'TreeView1tX'
	button_id = button_id.replace('X', str(id))
	a = html.cssselect('a#'+button_id)[0]
	# extract numerical list symbols
	m = re.search('((\d)(\.\d)?)', a.text_content())
	category = m.group(2)
	sub_category = m.group(0)
	# remove numbers from label
	label = re.sub('(\d+|\.)', '', a.text_content()).strip()
	buttons[button_id] = [category, sub_category, label]

print(buttons)

state_options = {
	'Andhra Pradesh' : '01',
	'Assam' : '02',
	'Bihar' : '03',
	'Gujarat' : '04',
	'Haryana' : '05',
	'Himachal Pradesh' : '06',
	'Jammu and Kashmir' : '07',
	'Karnataka' : '08',
	'Kerala' : '09',
	'Madhya Pradesh': 10,
	'Maharashtra' : 11,
	'Manipur' : 12,
	'Meghalaya' : 13,
	'Nagaland' : 14,
	'Orissa' : 15,
	'Punjab' : 16,
	'Rajasthan' : 17,
	'Tamil Nadu' : 18,
	'Tripura' : 19,
	'Uttar Pradesh' : 20,
	'West Bengal' : 21,
	'Sikkim' : 22,
	'Chhattisgarh' : 23,
	'Jharkand' : 24,
	'Uttarakhand' : 25,
	'Telangana' : 26,
	'Andaman and Nicobar Islands' : 31,
	'Arunachal Pradesh' : 32,
	'Chandigarh' : 33,
	'Dadra and Nagar Haveli' : 34,
	'NCT of Delhi' : 35,
	'Daman and Diu': 36,
	'Lakshadweep' : 37,
	'Mizoram' : 38,
	'Pondicherry' : 39,
	'Goa' : 40,
}

viewstate = html.cssselect("input#__VIEWSTATE")[0].value
# viewstate should be persistent, because there is no javascript being loaded
event_validation = html.cssselect("input#__EVENTVALIDATION")[0].value
print(viewstate)
print(event_validation)

### PROBLEM IS VIEWSTATE GETS MODIFIED BY CLICKING
for state, state_id in state_options.items():
	for year in option_vals:
		for button, value_list in buttons.items():
			xls_filename = '-'.join(state.split(' '))+'_'+year+'_'+'-'.join(re.sub('\s*(\)|\(|\.|\,)\s*', '', label).split(' '))+'.xls'
			if path.isfile(path.join(data_dir, xls_filename)):
				continue
			event_argument = 'sxyz\Part - X' # <-- modify (1-3)\\(1-3)\.(1-6)
			category = value_list[0]
			sub_category = value_list[1]
			label = value_list[2]
			event_argument = event_argument.replace('X', str(category)+'\\'+str(sub_category))
			selected_node = button
			params = {
				'TreeView1_ExpandState' : 'eenennnnennnnnn',
				'TreeView1_SelectedNode' : selected_node,
				'__EVENTTARGET' : 'TreeView1',
				'__EVENTARGUMENT' : event_argument,
				'__VIEWSTATE' : viewstate,
				'__EVENTVALIDATION' : event_validation,
				'TreeView1_PopulateLog' : '',
				'DropDownList1' : str(state_id),
				'DropDownList2' : year,
				'DropDownList3' : '2' # return excel
                        }
                        resp = s.post(url, stream=True, data=params, headers=item_request_headers)
                        print(resp.headers)
                        print(resp.text)
			# request headers
			#print(resp.request.headers)
			# content field holds response as bytestring!
			xls_file = resp.content
			print(xls_filename)
			# save bytes to file --> no need for Response.iter_content, because files are small
			with open(path.join(data_dir, xls_filename), 'wb') as f:
				f.write(xls_file)
				f.close()

"""
	opener = urllib2.build_opener()
    opener.addheaders.append(('Cookie', 'ASP.NET_SessionId=' + cookie))
    req = opener.open(main_url, params)
    result_html = req.read()
"""
