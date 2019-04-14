import sys
import time
sys.path.append('/home/saulo/projects/cpl/src')

import pandas as pd
import cpl.promotion as pr
from cpl.utils import db, db_ap, db_onto, cpl_conf, flatten


def main():
    
    num_iter = cpl_conf.num_iter        # number of iterations
    max_p = cpl_conf.max_p_promotions   # max pattern promotions per iteration
    max_i = cpl_conf.max_i_promotions   # max instance promotions per iteration
    l = cpl_conf.limit                  # max number of positive candidates for promotion
    T = cpl_conf.T                      # multiplier of promotion threshold
    
    #load category metadata
    categories_init = db_onto.find(projection=['category_name',
                                               'seed_instances',
                                               'seed_ctx_pattern'])
    
    for i in range(num_iter):  # for i iterations
        
        i_start_time = time.time()
        
        df_all_promoted_instances = pr.all_promoted_instances()
        df_all_promoted_patterns = pr.all_promoted_patterns()
        
        categories_init.rewind()
        
        if (i == 0):  #if first iteration
            pr.promote_seeds(categories_init)
        else:
            for c_init in categories_init:  # for all categories
                
                #load category information
                category = db_onto.find_one({'category_name':c_init['category_name']})
                
                if i <= len(category['promoted_patterns']):  #if there are positive patterns for this iteration
                    start = time.time()
                    pi = pr.promote_instances(category, i, max_i, l, T, df_all_promoted_patterns)
                    end = time.time()
                    print('instance',i, c_init['category_name'], len(pi), end-start, sep=',')
                    
                if i <= len(category['promoted_instances']):  #if there are positive patterns for this iteration
                    start = time.time()
                    pp = pr.promote_patterns(category, i, max_p, l, T, df_all_promoted_instances)
                    end = time.time()
                    print('pattern',i, c_init['category_name'], len(pp), end-start, sep=',')
                    
        i_end_time = time.time()
        
    db.close()
    
if __name__ == '__main__':
    main()