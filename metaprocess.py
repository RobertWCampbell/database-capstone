# download file from Statistics Canada for a given table number

# import packages
import os
import psycopg
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
# -------------------------------------------------------------------
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
db = os.getenv("POSTGRES_DB")

conn_str = f"postgresql://{user}:{password}@db:5432/{db}"

logging.info("Dimension heirarchy: extract data from staging area db")
with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                    DROP TABLE IF EXISTS load.memparents;

                    CREATE TABLE load.memparents AS SELECT members.*, isparent
                    FROM load.Members LEFT  JOIN (
                    SELECT DISTINCT  members.dimensionpositionid, members.parentmemberid, 
                                1 as isparent
                        FROM load.Members) as parents
                        ON members.memberid = parents.parentmemberid 
                            AND members.dimensionpositionid = parents.dimensionpositionid;
                        """)

            cur.execute("""
                    SELECT DISTINCT  members.dimensionpositionid, members.memberid, 
                                members.parentmemberid 
                            FROM load.memparents as members
                            WHERE members.isparent is NULL;
                    """)
            leaves = cur.fetchall()

            cur.execute("""
                    SELECT DISTINCT  members.dimensionpositionid, members.memberid, 
                    members.parentmemberid 
                            FROM load.memparents as members;
                    """)
            tree = cur.fetchall()


#######################################################################################
# dimension hierarchy crawl
#######################################################################################

#######################################################################################
# simple crawl through dimension hierarchy
#######################################################################################
dim = {} 
parents={}
dd=[]

for t in tree:
    if t[0] not in parents.keys():
        parents[t[0]]={}
    parents[t[0]][t[1]]=t[2]

for l in leaves:
    if l[0] not in dim.keys():
        dim[l[0]]=[]
    dim[l[0]].append(list(l))

def updates(dd):
    for d in dd:
        if d[-1] != None:
            d.append(parents[d[0]][d[-1]])
            if parents[d[0]][d[-2]] not in up:
                up.append(parents[d[0]][d[-2]])

for k, dd in dim.items():
    up = []
    updates(dd)
    if len(up) > 1:
        updates(dd)

print("should be a list of lists...")
print(dim)
