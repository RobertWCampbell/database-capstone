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
                            FROM load.memparents as members;
                    """)
            tree = cur.fetchall()


#######################################################################################
# dimension hierarchy crawl
#######################################################################################

logging.info("Dimension heirarchy structure crawl")

def makeParent(d: list) -> dict:
    """
    Assume list of n tuples of three: dimensionID, memberID, partentID
    """
    dim = dict()
    for dd in d:
        if dd[0] not in dim:
            dim[dd[0]]=dict()
        dim[dd[0]][dd[1]]=dd[2]
    return dim

#parents = makeParent(tree)

def t2l(d):
    """Make a list of tuples to a list of lists --> mutable"""
    ll = list()
    for dd in d:
        ll.append(list(dd))
    return ll

def findParents(d: list, p: dict) -> list:
    """Finds the parentage of a member"""
    pp = list()
    parentDict = dict()
    for k in p.keys():
        parentDict[k] = list()

    for dd in d:
        while dd[-1] != None and p[dd[0]][dd[-1]] != None:
            dd.append(p[dd[0]][dd[-1]])
        if dd[-1] == None:
            dd.append(0)
        else:
            dd.append(len(dd)-2)
        pp.append(dd)
        if dd[2] not in parentDict[dd[0]]:
            parentDict[dd[0]].append(dd[2])

    final = list()
    for ppp in pp:
        if ppp[1] in parentDict[ppp[0]]:
            ppp.append(1)
        else:
            ppp.append(0)
        final.append(ppp[:2]+ppp[-2:])

    return final

d = t2l(tree)
p = makeParent(tree)
print(p)
#alldim = findParents(t2l(tree), makeParent(tree))
alldim = findParents(d, p)

print('sorted:')
print(sorted(alldim))


logging.info("Dimension heirarchy: load new tables")
with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS load.parents;
                CREATE TABLE load.parents (
                    dimensionpositionid INTEGER,
                    memberid INTEGER,
                    level INTEGER,
                    isparent INTEGER);
            """)
            conn.commit()

            for f in alldim:
                cur.execute("""
                            INSERT INTO load.parents (dimensionpositionid, memberid, isparent, level)
                                VALUES(%s, %s, %s, %s) """, f) 

            conn.commit()

            cur.execute("""
                    DROP TABLE IF EXISTS load.memparents;

                    CREATE TABLE load.memparents AS SELECT members.*, parents.isparent, parents.level
                    FROM load.Members LEFT  JOIN load.parents
                        ON members.memberid = parents.memberid 
                            AND members.dimensionpositionid = parents.dimensionpositionid;
                        """)
            
            conn.commit()
