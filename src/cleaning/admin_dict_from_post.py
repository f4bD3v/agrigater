import csv
import re
import string
import pickle
import pandas as pd
import Levenshtein

# state: district: [taluk]
hierarchy_dict = {}
# pin: [(state, district, taluk)+]
pin_dict = {}

dist_by_taluk_dict = {}

corrections = {'Viskhapatnam': 'Visakhapatnam',
               'Sikandarabad': 'Sikandrabad',
               'The Dangs': 'Dang',
               'Mohali': 'Sahibzada Ajit Singh Nagar'}


def filter_row(row):
    pin = row[1]
    taluk = re.sub(';', ',', row[7])
    district = row[8]
    state = string.capwords(row[9])
    state = state.replace('&', 'and')
    return (pin, (state, district, taluk))


def main():
    #po_csv = '../../data/admin/all_india_POs_Telangana.csv'
    #df = pd.DataFrame.from_csv(po_csv)

    with open('../../data/admin/all_india_POs_Telangana.csv', 'r') as admin_csv:
        rows = csv.reader(admin_csv)
        next(rows, None)
        for row in rows:
            (pin, address) = filter_row(row)
            if address[2] == 'NA' or address[2] == 'Na':
                address = (address[0], address[1], '*')
            address = '; '.join(address)
            # introduce whitepsace after punctutation
            for p in string.punctuation:
                if p == '*':
                    continue
                address = address.strip(p)
                if p == '-':
                    address = address.replace(p, ' ')
                else:
                    address = address.replace(p, p+' ')
            address = re.sub('\s+', ' ', address).strip('')
            # remove excessive whitespace
            for key, value in corrections.iteritems():
                address = re.sub(key, value, address, flags=re.IGNORECASE)
            address = string.capwords(address)
            address = tuple(address.split('; '))
            if pin in pin_dict:
                pin_dict[pin] = pin_dict[pin]|set([address])
            else:
                pin_dict[pin] = set([address])
            (state, district, taluk) = address
            if state in hierarchy_dict:
                if '(' in district:
                    print district
                    district = re.sub(r'\(.*\)', '', district).strip()
                if '(' in taluk:
                    print taluk
                if district in hierarchy_dict[state]:
                    #if 'Sukma' in district or 'Kutch' in district:
                    #    print district
                    #if Levenshtein.distance(district, 'Devbhumi Dwraka') <= 6:
                    #    print district
                    #if Levenshtein.distance(district, 'Chhotoudepur') <= 6:
                    #    print district
                    if taluk != '*':
                        if '(' in taluk:
                            taluk = re.sub(r'\(.*', '', taluk).strip()
                        #if Levenshtein.distance(taluk, 'Lalpur') <= 2:
                        #    print taluk, district
                        hierarchy_dict[state][district] = hierarchy_dict[state][district]|set([taluk])
                        if state in dist_by_taluk_dict:
                            if taluk == 'Ponda' and district == 'North Goa':
                                continue
                            if taluk in dist_by_taluk_dict[state]:
                                dist_by_taluk_dict[state][taluk] = dist_by_taluk_dict[state][taluk]|set([district])
                            else:
                                dist_by_taluk_dict[state][taluk] = set([district])
                        else:
                            dist_by_taluk_dict[state] = {}
                else:
                    hierarchy_dict[state][district] = set()
            else:
                hierarchy_dict[state] = {}

        pickle.dump(pin_dict, open('pin_dict.p', 'wb'))
        """
        for pin in pin_dict.keys():
            print pin
            for ad in pin_dict[pin]:
                print ad
            print ' '
        """
        pickle.dump(hierarchy_dict, open('admin_hierarchy_dict.p', 'wb'))
        pickle.dump(dist_by_taluk_dict, open('dist_by_taluk_dict.p', 'wb'))

if __name__ == "__main__":
    main()

