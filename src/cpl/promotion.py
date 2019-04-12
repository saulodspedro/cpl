from cpl.utils import flatten, db_cpl, db_ap
import pandas as pd

def promote_seeds(categories):
    
    for category in categories:
        db_cpl.ontology.update_one({'category_name':category['category_name']},
                                   {"$set":{'promoted_instances.0':category['seed_instances']}})
        
        db_cpl.ontology.update_one({'category_name':category['category_name']},
                                   {"$set":{'promoted_patterns.0':category['seed_ctx_pattern']}})

def promote_instances(category, iteration, max_promotions ,limit, T):
    
    last_promoted_patterns = category['promoted_patterns'][iteration-1]
    all_promoted_patterns = list(flatten(category['promoted_patterns']))
    all_promoted_instances = list(flatten(category['promoted_instances']))
    
    #get the promoted patterns of the mutex exception categories
    mutex_query = (db_cpl.ontology.
                   find({'category_name':{"$in":category['mutex_exceptions']}}))
    
    mutex_patterns = list(flatten([i['promoted_patterns'] for i in mutex_query]))

    #extraction step
    #count the ocurrences of instances that co-occur with the
    #positive promoted patterns in the last iteration
    #without considering instances that were already promoted
    pipe_pos = [{"$match":{"ctx_pattern":{"$in":last_promoted_patterns},
                           "noun_phrase":{"$nin":all_promoted_instances}}},     
                {"$group": {"_id": "$noun_phrase", "count": {"$sum": "$counter"}}}, 
                {"$limit": limit}]  # limit is used for performance only
    
    #count the ocurrences of instances that co-occur with negative patterns
    pipe_neg = [{"$match":{"ctx_pattern":{"$nin":all_promoted_patterns + mutex_patterns}}},
                {"$group": {"_id": "$noun_phrase", "count": {"$sum": "$counter"}}}]
    
    agg_pos = list(db_ap.allpairs.aggregate(pipe_pos))
    agg_neg = list(db_ap.allpairs.aggregate(pipe_neg))
    
    if (agg_pos and agg_neg):  #if at least one positive and one negative pattern was found
        df_positive = pd.DataFrame(agg_pos).set_index("_id")
        df_negative = pd.DataFrame(agg_neg).set_index("_id")
        
        joined = (df_positive.join(df_negative, rsuffix='_neg')
                  .fillna(0)
                  .assign(passed_filter=lambda df:  
                          (df['count'] >= df['count_neg']*T) &  # filter criterion #1
                          (df['count'] >= 2)))                  # filter criterion #2
        
        promoted_instances=list((joined[joined['passed_filter']]            # filter step
                                 .sort_values(by='count', ascending=False)  # rank step
                                 .head(max_promotions)                      # promote step
                                 .index.values))
        
        #update ontology with the promoted instances
        db_cpl.ontology.update_one({'category_name':category['category_name']},
                                   {'$set':{'promoted_instances.'+str(iteration):promoted_instances}})
    else:
        promoted_instances = []
    
    return promoted_instances
                  
def promote_patterns(category, iteration, max_promotions ,limit, T):
    
    last_promoted_instances = category['promoted_instances'][iteration-1]
    all_promoted_patterns = list(flatten(category['promoted_patterns']))
    all_promoted_instances = list(flatten(category['promoted_instances']))
    
    #get the promoted instances of the mutex exception categories
    mutex_query = (db_cpl.ontology.
                   find({'category_name':{"$in":category['mutex_exceptions']}}))
    
    mutex_instances = list(flatten([i['promoted_instances'] for i in mutex_query]))
    
    #extraction step
    #count the ocurrences of patterns that co-occur with the
    #positive promoted instances in the last iteration
    #without considering patterns that were already promoted
    pipe_pos = [{"$match":{"noun_phrase":{"$in":last_promoted_instances},
                           "ctx_pattern":{"$nin":all_promoted_patterns}}},
                {"$group": {"_id": "$ctx_pattern", "count": {"$sum": "$counter"}}},
                {"$limit": limit}]  # limit is used for performance only
    
    #count the ocurrences of patterns that co-occur with negative instances
    pipe_neg = [{"$match":{"noun_phrase":{"$nin":all_promoted_instances + mutex_instances}}},
                {"$group": {"_id": "$ctx_pattern", "count": {"$sum": "$counter"}}}]
    
    agg_pos = list(db_ap.allpairs.aggregate(pipe_pos))
    agg_neg = list(db_ap.allpairs.aggregate(pipe_neg))
    
    if (agg_pos and agg_neg):  #if at least one positive and one negative instance was found
        df_positive = pd.DataFrame(agg_pos).set_index("_id")
        df_negative = pd.DataFrame(agg_neg).set_index("_id")
        
        pipe_count = [{"$match":{"ctx_pattern":{"$in":list(df_positive.index.values)}}},
                      {"$group": {"_id": "$ctx_pattern", "count": {"$sum": "$counter"}}}]
        
        df_count = pd.DataFrame(list(db_ap.allpairs.aggregate(pipe_count))).set_index("_id")
        
        joined = (df_positive
                  .join(df_negative, rsuffix='_neg')
                  .join(df_count, rsuffix='_cnt')
                  .fillna(0)
                  .assign(passed_filter=lambda df:  
                          (df['count'] >= df['count_neg']*T) &  # filter criterion #1
                          (df['count'] >= 2))                   # filter criterion #2
                  .assign(precision=lambda df:
                          df['count']/df['count_cnt']))  #rank criterion
                  
        promoted_patterns = list((joined[joined['passed_filter']]                # filter step
                                  .sort_values(by='precision', ascending=False)  # rank step
                                  .head(max_promotions)                          # promote step
                                  .index.values))
        
        #update ontology with the promoted patterns
        db_cpl.ontology.update_one({'category_name':category['category_name']},
                                   {'$set':{'promoted_patterns.'+str(iteration):promoted_patterns}})
        
    else:
        promoted_patterns = []
        
    return promoted_patterns
    
    