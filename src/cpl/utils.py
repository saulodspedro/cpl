import pymongo
import collections
from cpl.cplconfig import configCPL

cpl_conf = configCPL()
    
#connection to mongo database
db = pymongo.MongoClient(cpl_conf.db_address)
db_onto = db['db_cpl'].ontology
db_ap = db['db_allpairs'].allpairs_counts

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el