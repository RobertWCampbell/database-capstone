# download file from Statistics Canada for a given table number

# import packages
import os
import psycopg2
import re
import urllib.request, urllib.parse, urllib.error
import http
import json
import ssl

# base URL
serviceurl = 'https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/'

tbl = input('Enter CANSIM NDM table number: ')

tbl = re.findall("[0-9]*",tbl.strip().replace("-", ""))
if type(tbl)==list:
    tbl = max(tbl)

tbl = tbl[:8]

# assume always English for now...
url = serviceurl + tbl + '/en'
print(url)

# try the table number with the API 
with urllib.request.urlopen(url) as uh:
    data = json.loads(uh.read().decode())
    newurl = data['object']

print(newurl)


postbody = json.dumps([{"productId": int(tbl)}]).encode()

print(postbody)

# Build request 
metaurl = 'https://www150.statcan.gc.ca/t1/wds/rest/getCubeMetadata'
req = urllib.request.Request(metaurl, data=postbody,
                             headers={"Content-Type": "application/json"}, 
                             method="POST" )

# try the table number with the API 
with urllib.request.urlopen(req) as uh:
    data = json.loads(uh.read().decode())
    data = data[0]['object']
    print(data.keys())

    
    for k,v in data.items():
        if type(v) == list:
            print(k)
            for i in v:
                print(type(i))
        else:
            print(k, type(v))

    #print(type(data[0].keys))
