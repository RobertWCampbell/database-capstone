# download file from Statistics Canada for a given table number

# import packages
import os
import psycopg3
import re
import urllib.request, urllib.parse, urllib.error
import http
import json
import ssl
import logging

# -------------------------------------------------------------------
# Logging setup
# -------------------------------------------------------------------
logging.basicConfig(
            level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(message)s"
                )


# -------------------------------------------------------------------
# base URL processing
# -------------------------------------------------------------------
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

# parse json file:
#    for k,v in data.items():
#        if type(v) == list:
#            print(k)
#            for i in v:
#                print(type(i))
#        else:
#            print(k, type(v))



# -------------------------------------------------------------------
# parse metadata and load data into staging area of database
# -------------------------------------------------------------------

# make tuples of the items I would like to capture from the metadata table
tblKeys = ('productId', 'cubeTitleEn', 'cubeTitleFr', 'cubeStartDate', 'cubeEndDate', 'releaseTime')
dimKeys = ('dimensionPositionId', 'dimensionNameEn', 'dimensionNameFr')
memberKeys = ('memberId', 'parentMemberId', 'memberNameEn', 'memberNameFr', 'terminated')

tblmeta = tuple(data[k] for k in tblKeys)

cur.execute('''INSERT INTO load.Cube (productId, cubeTitleEn, cubeTitleFr, cubeStartDate, cubeEndDate, releaseTime)
                    VALUES ( ?, ?, ?, ?, ?, ? )''', tblKeys)

# -------------------------------------------------------------------
# Database connection and prep the staging area for data loading
# -------------------------------------------------------------------
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
db = os.getenv("POSTGRES_DB")
#engine = create_engine(f"postgresql+psycopg://{user}:{password}@db:5432/{db}")
conn_str = f"postgresql://{user}:{password}@db:5432/{db}"
with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            
            cur.execute('''
                        DROP TABLE IF EXISTS load.Cube;
                        DROP TABLE IF EXISTS load.Dimensions;
                        DROP TABLE IF EXISTS load.Members;

                        CREATE TABLE Cube (
                            productId INTEGER PRIMARY KEY, 
                            cubeTitleEn TEXT, 
                            cubeTitleFr TEXT, 
                            cubeStartDate DATE, 
                            cubeEndDate DATE, 
                            releaseTime TIMESTAMP
                        );

                        CREATE TABLE Dimensions (
                            dimensionPositionId INTEGER PRIMARY KEY, 
                            dimensionNameEn TEXT, 
                            dimensionNameFr TEXT
                        );

                        CREATE TABLE Members (
                            dimensionPositionId INTEGER, 
                            memberId INTEGER, 
                            parentMemberId INTEGER, 
                            memberNameEn TEXT, 
                            memberNameFr TEXT, 
                            terminated INTEGER) 

                            PRIMARY KEY(dimensionPositionId, memberId)
                        );

                        ''')


            for d in data['dimension']:

                dd = tuple(d[k] for k in dimKeys)

                dimensionPositionId = d['dimensionPostionId']

                cur.execute('''INSERT INTO load.Dimensions (dimensionPositionId, dimensionNameEn, dimensionNameFr)
                                VALUES ( ?, ?, ? )''', dimKeys)

                for m in d['members']:
                    mm = (dimensionPositionId,) +  tuple(m[k] for k in memberKeys)

                cur.execute('''INSERT INTO load.Members (dimensionPositionId, memberId, parentMemberId, memberNameEn, memberNameFr, terminated)
                                VALUES ( ?, ?, ?, ?, ?, ? )''', memberKeys)

