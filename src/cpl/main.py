import sys
import time
sys.path.append('/home/saulo/projects/cpl/src')

from cpl.utils import db, db_cpl, cpl_conf
from cpl.promotion import promote_instances, promote_patterns, promote_seeds

def main():
    
    num_iter = cpl_conf.num_iter        # number of iterations
    max_p = cpl_conf.max_promotions     # max promotions per iteration
    l = cpl_conf.limit                  # max number of positive candidates for promotion
    T = cpl_conf.T                      # multiplier of promotion threshold
    
    #load category metadata
    categories_init = db_cpl.ontology.find(projection=['category_name',
                                                       'seed_instances',
                                                       'seed_ctx_pattern'])
    
    for i in range(num_iter):  # for i iterations
        
        i_start_time = time.time()
        
        if (i == 0):  #if first iteration
            promote_seeds(categories_init)
            categories_init.rewind()
        else:
            for c_init in categories_init:  # for all categories
                
                #load category information
                category = db_cpl.ontology.find_one({'category_name':c_init['category_name']})
                
                if i <= len(category['promoted_patterns']):  #if there are positive patterns for this iteration
                    start = time.time()
                    pi = promote_instances(category, i, max_p, l, T)
                    end = time.time()
                    print('instance',i, c_init['category_name'], len(pi), end-start, sep=',')
                    
                if i < len(category['promoted_instances']):  #if there are positive patterns for this iteration
                    start = time.time()
                    pp = promote_patterns(category, i, max_p, l, T)
                    end = time.time()
                    print('pattern',i, c_init['category_name'], len(pp), end-start, sep=',')
                    
        i_end_time = time.time()
        
        print('Iteration {} took {}s'.format(i, i_end_time-i_start_time))
        
    db.close()
    
if __name__ == '__main__':
    main()