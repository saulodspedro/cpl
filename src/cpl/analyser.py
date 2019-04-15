import sys
sys.path.append('/home/saulo/projects/cpl/src')

import argparse
from cpl.utils import db_onto, db_res

parser = argparse.ArgumentParser()
parser.add_argument('--it', help='iteration')
args = parser.parse_args()

i = int(args.it)

ontology = db_onto.find()

for o in ontology:
    try:   
        for pattern in o['promoted_patterns'][i]:
            ans = input(o['category_name']+": "+pattern+" ")

            db_res.insert_one({'category_name':o['category_name'],
                               'ctx_pattern':pattern,
                               'score':float(ans)})
    except(IndexError):
        print('Nothing for {}\n'.format(o['category_name']))