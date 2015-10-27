import requests
import lxml.html
import pickle

"""

pin_url = 'http://www.pincodein.com/Pin-Code-Search.aspx'
r = requests.get(pin_url)
cookie = r.cookies
payload = '__VIEWSTATE=hI3JwJsu3uoUt6bh6RW11SUKgvY4Bhc9ptcGtqt5igfahPOKJVVw%2FX3kjfpM8px%2BCpT%2FsIca2BZaTl0tFTIWQh95cSQ9T1egqylqfQfLOeAGPlXhqVPyAQAvP3p3tVEcr0p8ShatB37r31FbsjNX9NrgPhdY%2BBCLn0pinSEFKjHTkmh9GaWLY2F4reW52Q3ECpj74OJX%2FHXx0DLWaoc9%2Byk5Qajnzq9nOFAsUt6ZgYGXAU9CmypxgU%2BCSBa8dErS2uUAOS8WZI5ZRofCexjx4DOlNWD08%2F%2BYhVZSLrJTQcC1eVZNtgJS83n9DqaP4iryw9aiaLOzjrSYgVLFGBHG5qk1frkxYyocXXODGNrezhtTe4skXI1WS4LUoJN%2BV0go5TlpMMs8BqoxZfDnnzW0n6n6xWUeeh4JG7Jx30XRb5Cq8K7LBYxY%2Fqogmmc9ET2rA0Z87pBBupexpolh8JQ0WZYxx3INdEIzpXtK83n6cHptZgQuDZ6Q1ywZwnUpnEvEAxN8EtuXEf%2BBxIK9XYhN0cp%2FmpGF3hE2scArIb%2BFR3%2F4wr%2B%2FZ0xJXfZmx2hJZpyw%2F90rmQGMBQyJQp3VwtJzFfKoX811R3E4n5S7Vv7tjMKl2MmskK2CjC9a3sh6R%2BTiyWN1ABHseGjSGPtT5wbu9glr3qXfK2PZJrshtqriQVjK2NtmRq52Wmq7TfV23bRttquaBAJ92NNkvJ3VGZ6%2B91Y1jzGwD9YAVSegwmES0Umi8D8Zb0liO78Yts3p4jlN4D6UB5TIAmeVs5TbMi6jfva%2FBRcFbUwncbs%2FhraH9ObY%2FgmKoUhr%2FlkGKkOHkNI7ZAFZOJznsY0vB8GJqomyc%2FFWT1BN4dawQGItjJtbba%2FCtVrEONs6kZ4aNn2NtBcTZ5%2FGiHiYRt4zgayH8fkTvfGmrPIfTwehQiJNjtaWjk3LZBvJdmJzg8xIoQVa1tIN4vZqK75%2FXzwlfYlnxj7pz%2BiAvDXAwW3NZi5D4RoLS1e84VXi8v6EDecXgjNldl1d6fFOJnXPdBdM6Xh97Dzqmf%2B6OCyjZW8HwEgI2vHkq8KHDyQJ9g1B18gJRfwggYJzfxlR%2B%2FpAqxtQMOmipHdxpXS9hTTCRJzsrLY%2BF0GfiYYf1Y5Z9Vrfcq1QSxjjHQOEVD8VfUMBkzpcm30TZTN4W%2FL4W9%2Fa%2BJuIKx0IBkqnToFKlWu5x%2BX3MGYC59rILI1MnC2knQf2z0FWlhyEuV%2FdMz%2BbMmxIn1lFuBtYUenFgfpK%2FCVk5GFXa7%2F%2BL1L12Q%2F2sV1QfcJnwNykAfz3%2BkmmbpIIy39XT78Uq%2FnMtraNtVHS8fW2kW1rUFOZ2Zg2%2FOCu7nv1%2B8AAhPLpyCxgFB0mmLj4kRL54RW6SdshnqZYKLzxt1kbm4unKVkk7CWL62rrQ4WftP9x3LTBlHat%2FhZxH9XounWidYPf6nBkA%2FP17YN5zlaXl3YRC9f0EAZmXlXzTjuAdz7qrU4q5MkDzYn3lT%2BC3tyOF%2Fogmyn%2Fzi5PcxA9mAQHvtmoLX6Ab2mgbQzE6wHPXMBjYV2uNhyogftMu0Tnt5MNVtMK04hz8qobExys%2BrTMQ1XK%2FiHrpCqNzTT6oeoLny%2FoeUr8oWA1GULYVtbbn3W2YXth1yOPpcM6sEhsF5Zfo67TieXQz4HdM0WStirXk0y4s6dOD9sCrbBQZmwJTEy3wB4Dtdc0hkyWZuYuHlNMUIkHsdBEAcXrvc14%2Bqot8VLXXRghZtCZLL%2BMr0heT4ypkqrrOXaH3929k7%2FRxV7naBqBo5Yzi%2FRyXYzRntZLXTsakkNTlphaCdEocKCn%2BcmmWigAVyLmBkAFkaKaIDROY%2BWbwnkXv3WwCFNNEcujfbbZI%2BLcE944S9vdXt1BAN0fbRVEh7n8mwSe8UJQv94BjReTF4xz98OJfh0tXnGNyxPEiLF8B3Rq9WYpRvS%2BSvyLk%2FOf6WwU2mMCQPHDW4fezxj%2FYYV04H3DWVBs0A3F%2FVAnzE906Cs%2FTt9qVr58eCaZ823LzoKFfOgyzZdmodHT5KDFNcHmT1ZEh7%2BYpMiyut3CA1jZzQSRTLclasL37n2x8TWMQhxgpMoT4Llreqa48XRhx%2FgNaUQaxWIN7ks2QuX4%2F%2BbtD50CijcTXb9bgnCXbNuGQgtY5cUhpGA%2F3OXcCqlWM7itzgDGSNfyUCnB4wLNuE%2FyvUXl4eW%2Bhxrpi6qS8btWM3euDYxTJQQDxAbdXfvggGZpO%2FTL3KyuRrIq8nmgOXtWGUxIvq1qKDDkHLGtfz9%2BTEFdt3z3t7o%3D&__VIEWSTATEGENERATOR=81B8C5D7&__VIEWSTATEENCRYPTED=&__EVENTVALIDATION=QxykRWOvKey%2BK0f1bagFUu%2B1DUQlRT8zh1ruXrKszgK%2Fvkkf%2BjhETYJ5SJkUrwPBUOvOUjnj%2Bl0uGGOL6qXtYYPE%2FINjQKLIyN5j%2F8s7CRyHwJvjI0cSUNWZMSE6qQNI%2BesyzyELgA1sBe%2F1maEF5EZnsWoFSuZIZLwT5zqE5P0%3D&ctl00%24BC%24txtPinCode=#{TXTPIN}'
payload = payload.replace('#{TXTPIN}', pin)
headers = { 'Cookie': '__utmt=1; __utma=14594870.1355084490.1442763675.1442763675.1442843763.2; __utmb=14594870.6.10.1442843763; __utmc=14594870; __utmz=14594870.1442763675.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)' }
#retrieval does not work with requests
#r = requests.post(pin_url, data=payload, headers=headers)
#print r.text
opener = urllib2.build_opener()
opener.addheaders.append(('Cookie', cookie))
req = opener.open(pin_url, payload)
result_html = req.read()
print result_html
pin = '515571'
"""

def get_address_by_pin(pin):
    pin_dict = pickle.load(open('pin_dict.p', 'rb'))
    if not pin:
        return (set(), set(), set())
    pin = str(pin[0]) #str(pin.pop())
    if pin in pin_dict:
        return pin_dict[pin]
    else:
        return (set(), set(), set())

def request_address_by_pin(pin, taluk_names):
    if not pin:
        return ()
    pin = str(pin.pop())
    # citypincode.co.in/#{PINCODE} looks like a better option, because there are
    # no restrictions on data use
    # https://data.gov.in/catalog/all-india-pincode-directory#web_catalog_tabs_block_10
    # data.gov.in data is not up to date (no Telangana) and part of the taluk
    # names are abbreviated # downloaded wrong document
    pin_url = 'http://www.pincodein.com/pin-code-#{PINCODE}'
    pin_url = pin_url.replace('#{PINCODE}', pin)
    r = requests.get(pin_url)

    # lxml find first pin code
    html = lxml.html.fromstring(r.text)
    #html.cssselect(tag) if pin in td.textcontent()
    td_titles = html.cssselect('td.DDLH')
    # what if pincode can't be found?:
    # --> no td_titles
    if not td_titles:
        # discard pin in caller
        return (set(), set(), set(), pin)
    # find all td with class DDLH
    district_titles = [td for td in td_titles if 'District' in td.text_content()]
    taluk_titles = [td for td in td_titles if 'Taluk' in td.text_content()]
    state_titles = [td for td in td_titles if 'State' in td.text_content()]

    # merge results in sets
    districts = set()
    taluks = set()
    states = set()
    # then take text_content of next td respectively
    for i in range(len(district_titles)):
        print district_titles[i]
        print district_titles[i].getnext()
        # avoid NAs
        text = district_titles[i].getnext().text_content().strip()
        if text != 'NA':
            districts.add(text)
        text = taluk_titles[i].getnext().text_content().strip()
        if text != 'NA':
            print text
            for taluk in taluk_names:
                if taluk == text:
                    print taluk
                    taluks.add(text)
        text = state_titles[i].getnext().text_content().strip()
        if text != 'NA':
            states.add(text)
    return (states, districts, taluks, pin)

def main():
    print address_items_by_pin('515571')

if __name__ == '__main__':
    main()
