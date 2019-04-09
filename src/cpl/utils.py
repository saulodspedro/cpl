import pymongo
import collections
from cpl.cplconfig import configCPL

def db_conn(db_name):
    
    cpl_conf = configCPL()
    
    #connection to mongo database
    conn = pymongo.MongoClient(cpl_conf.db_address)

    return conn[db_name]

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el