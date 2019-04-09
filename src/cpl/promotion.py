from cpl.utils import flatten, db_conn

def promote_instances(category, iteration, max_promotions ,limit, T):
    
    db_ap = db_conn('db_allpairs')
    db_cpl = db_conn('db_cpl')
    
    last_promoted_patterns = category['promoted_patterns'][iteration]
    all_promoted_patterns = list(flatten(category['promoted_patterns']))
    all_promoted_instances = list(flatten(category['promoted_instances']))
    
    #get the promoted patterns of the mutex exception categories
    mutex_query = (db_cpl.ontology.
                   find({'category_name':{"$in":category['mutex_exceptions']}}))
    
    mutex_patterns = list(flatten([i['promoted_patterns'] for i in mutex_query]))

    #count the ocurrences of all instances promoted pattern (positive) for this iteration
    pipe_pos = [{"$match":{"$and":[{"ctx_pattern":{"$in":last_promoted_patterns}},       # extraction step
                                   {"noun_phrase":{"$nin":all_promoted_instances}}]}},   # filter already promoted
                {"$group": {"_id": "$noun_phrase", "count": {"$sum": "$counter"}}}, 
                {"$sort": {"count":-1}},                                                 # rank step
                {"$limit": limit}]  # limit is used for performance only
    
    #count the ocurrences of all instances promoted pattern (negative) for this iteration: filter step
    pipe_neg = [{"$match":{"$and":[{"ctx_pattern":{"$nin":all_promoted_patterns}},
                                   {"ctx_pattern":{"$nin":mutex_patterns}}]}},
                {"$group": {"_id": "$noun_phrase", "count": {"$sum": "$counter"}}},
                {"$sort": {"count":-1}}]
    
    if list(db_ap.allpairs.aggregate(pipe_pos)):  #if at least one positive pattern was found
        df_positive = pd.DataFrame(list(db_ap.allpairs.aggregate(pipe_pos))).set_index("_id")
        df_negative = pd.DataFrame(list(db_ap.allpairs.aggregate(pipe_neg))).set_index("_id")
        
        joined = (df_positive.join(df_negative, rsuffix='_neg')
                  .fillna(0)
                  .assign(mult_count_neg=lambda df: df['count'] >= df['count_neg']*T))
        
        promoted_instances=list(joined[joined['mult_count_neg']]
                                .head(max_promotions)  # promote step
                                .index.values)
    else:
        promoted_instances = []
        
    db_ap.close()
    db_cpl.close()
    
    return promoted_instances