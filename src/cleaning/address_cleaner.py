import re
import string
import Levenshtein
import ngram

"""
    http://streamhacker.com/2011/10/31/fuzzy-string-matching-python/
    - write string normalization function
    - check out difflib
    - check out fuzzywuzzy
    - nltk normalize and fuzzy match implementation
"""

dist_match = ''

def find_pin(address):
    rgx=r'\d{6}'
    match = re.search(rgx, address)
    if match is None:
        rgx=r'\d{3}\s\d{3}'
        match = re.search(rgx, address)
        if match is None:
            return None
        return ''.join(match.group(0).split())
    else:
        return match.group(0)

def normalize(s, states):
    if not s:
        return s
    for p in string.punctuation:
        # keep commas for address item specific regex
        if p in ',':
            s = s.replace(p, ' '+p+' ')
            continue
        s = s.replace(p, ' ')

    s = re.sub('\d+', ' ', s)
    s = re.sub('\sPin\s', ' ', s, flags=re.IGNORECASE)

    for state in states:
        if state in s:
            s = s.replace(state, '').rstrip().rstrip(',').rstrip()

    # insert space in front of upper case letters
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    # with look behind: re.sub(r"(?<=\w)([A-Z])", r" \1",
    s = re.sub('\s+', ' ', string.capwords(s)).strip()
    s = re.sub('Disst', 'Dist', s, flags=re.IGNORECASE)
    s = re.sub('Dis ', 'Dist ', s, flags=re.IGNORECASE)
    # use .lower() ?
    # makes recognition of initials without punctuation more error prone?
    # WG -> wg, EG -> eg
    return s

def hasNumbers(s):
    return any(char.isdigit() for char in s)

def to_initials(name):
    # re does not captures repeated captures, only last match
    if not hasNumbers(name):
        rgx=r'^([A-Z])\w+(?:\s+([A-Z])\w+)?(?:\s+([A-Z])\w+)?'
        match = re.search(rgx, name)
        if match is None:
            print('cannot extract initials from: %s'%name)
            return []
        num_in = len(match.groups())
        in_list = []
        var_initials = []
        for i in range(num_in):
            if not match.group(i+1):
                break
            in_list.append(match.group(i+1).upper())
        if len(in_list) < 2:
            return []
        #var_initials.append('. '.join(in_list)+'.')
        #var_initials.append('. '.join(i.lower() for i in in_list)+'.')
        #var_initials.append('.'.join(in_list)+'.')
        #var_initials.append('.'.join(i.lower() for i in in_list)+'.')
        var_initials.append(' '+' '.join(i.lower() for i in in_list)+' ')
        var_initials.append(' '+' '.join(in_list)+' ')
        var_initials.append(' '+''.join(in_list)+' ')
        var_initials.append(' '+in_list[0]+' '+' '.join(name.split()[1:])+' ')
        # no ignore case regex for initials
        return var_initials
    return []

def find_initials(address, initials):
    found_districts = []
    districts = initials.keys()
    for dist in districts:
        for init in initials[dist]:
            # 'in' lookup by default case sensitive
            if init in address:
                found_districts.append(dist)
    return list(set(found_districts))


def ngram_similarity(s1, s2):
    return ngram.NGram.compare(s1.lower(), s2.lower())

def fuzzy_match(s, dist, max_dist = 2):
    if 'west' in s.lower() or 'east' in s.lower():
        max_dist = 1
    if 'upper' in s.lower() or 'lower' in s.lower():
        max_dist = 2
    return Levenshtein.distance(s.lower(), dist.lower()) <= max_dist

def find_taluk(taluks, address, repeat):
    assigned_taluks = []
    rgx=r'(?P<before>(\w+\s+){0,%s})\S*(tehsil|taluk)\S*(?P<after>(\s+\w+){0,%s})'%(repeat,repeat)
    match = re.search(rgx, address, re.IGNORECASE)
    if match is None:
        return assigned_taluks
    before = re.sub('\d', '', match.group('before')).strip()
    after = re.sub('\d', '', match.group('after')).strip()
    for taluk in taluks:
        if fuzzy_match(before, taluk, 1):
            sim = ngram_similarity(before, taluk)
            assigned_taluks.append((taluk, sim))
        if fuzzy_match(after, taluk, 1):
            sim = ngram_similarity(after, taluk)
            assigned_taluks.append((taluk, sim))
    return assigned_taluks

def find_district(districts, address, repeat):
    assigned_districts = []
    rgx=r'(?P<before>(\w+\s+){0,%s})\S*(dist|dt)\S*(?P<after>(\s+\w+){0,%s})'%(repeat,repeat)
    # there will be a match if string is not empty
    # re.match only searches from beginning of string
    match = re.search(rgx, address, re.IGNORECASE)
    if match is None:
        return assigned_districts
    # (" ".join(match.group('before').split()), " ".join(match.group('after').split()))
    before = match.group('before').strip()
    after = match.group('after').strip()
    for dist in districts:
        dist_match = ''
        if fuzzy_match(before, dist):
            dist_match = before
            sim = ngram_similarity(before, dist)
            print before
            print dist, sim
            assigned_districts.append((dist, sim))
        if fuzzy_match(after, dist):
            dist_match = after
            sim = ngram_similarity(after, dist)
            print after
            print dist, sim
            assigned_districts.append((dist, sim))
    return assigned_districts

# call for each address (APMC + Secretary) and then do a union of results
def assign_district(address, districts_by_state, initials, corrections = {}):
    assigned_districts = []
    # check if string is empty
    if address:
        ### DO CORRECTIONS PRIOR BY DICT REGEX REPLACEMENT ON DF
        for key, value in corrections.iteritems():
            if key in address:
                print('%s %s'%(key, value))
                address = re.sub(key, value, address)
        # can't split because of multiple word district names
        # some more cleaning?
        # collect misspellings in file? gather all misspellings of
        # districts in file automatically and then filter manually
        for dist in districts_by_state:
            if dist in address:
                assigned_districts.append(dist)
            # keep longest matching string
            if assigned_districts:
                return max(assigned_districts, key=len)

        if not assigned_districts:
            # heuristic, search for district name
            # set window around 'dist' to 2
            # return best match according to ngram distance
            assigned_districts = find_district(districts_by_state, address, 2)
            if assigned_districts:
                print(assigned_districts)
                assigned_districts.sort(key=lambda x: x[1],reverse=True)
                return assigned_districts[0][0]
            if not assigned_districts:
                # return districts contained in whole address
                assigned_districts = find_initials(address, initials)
                if assigned_districts:
                    return assigned_districts[0]
                if not assigned_districts:
                    address_elems = address.split(', ')[-1].split(' ')
                    for elem in address_elems:
                        for dist in districts_by_state:
                            if fuzzy_match(elem, dist, 1) and not (('Bundi' == dist or 'Banda'== dist or 'Gonda' == dist)  and 'Mandi' == elem) and elem != 'Khada':
                                # choose best match according to ngram distance
                                sim = ngram_similarity(elem, dist)
                                assigned_districts.append((dist, sim))
                    if assigned_districts:
                        assigned_districts.sort(key=lambda x: x[1],reverse=True)
                        return assigned_districts[0][0]
                    # + write elems with levenshtein <= 5 to file for manual correction mapping - do this in extra method?
    else:
        return None


def assign_taluk(address, taluks_by_state, market=''):
    assigned_taluks = []
    if address:
        # rgx tehsil check
        assigned_taluks = find_taluk(taluks_by_state, address, 1)
        if assigned_taluks:
            assigned_taluks.sort(key=lambda x: x[1],reverse=True)
            return assigned_taluks[0][0]
        if not assigned_taluks:
            #district = dist_match.pop()
            #dist.append(district)
            print('distmatch %s: '%dist_match)
            if dist_match:
                print('dist match: %s'%dist_match)
                occ = re.findall(dist_match, address)
                if len(occ) == 1:
                    address = re.sub(district, '', address).rstrip().rstrip(',')
                    address = re.sub('\s+', ' ', address)
        #address = address.rsplit(', ', 1)[0]
        if not assigned_taluks:
            if market:
                if market == 'Doranpal':
                    market = 'Dornapal'
                for taluk in taluks_by_state:
                    if taluk in market:
                        assigned_taluks.append(taluk)
                    # keep longest matching string
                    if assigned_taluks:
                        return max(assigned_taluks, key=len)
            if not assigned_taluks:
                for taluk in taluks_by_state:
                    if taluk in address and not ((taluk+' Road') in address) and not ((taluk+' Raj') in address):
                        assigned_taluks.append(taluk)
                    # keep longest matching string
                    if assigned_taluks:
                        return max(assigned_taluks, key=len)
                    if not assigned_taluks:
                        address_elems = address.split(' ')
                        for elem in address_elems:
                            for taluk in taluks_by_state:
                                if fuzzy_match(elem, taluk, 1) and not (elem == 'Konya' and taluk == 'Konta'):
                                    sim = ngram_similarity(elem, taluk)
                                    assigned_taluks.append((taluk, sim))
                        if assigned_taluks:
                            assigned_taluks.sort(key=lambda x: x[1],reverse=True)
                            return assigned_taluks[0][0]
    else:
        return None

#def simplify_market_name(state, market_name):
