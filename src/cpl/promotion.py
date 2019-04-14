from cpl.utils import flatten, db_onto, db_ap
import pandas as pd

def promote_seeds(categories):
    
    for category in categories:
        db_onto.update_one({'category_name':category['category_name']},
                           {"$set":{'promoted_instances.0':category['seed_instances']}})
        
        db_onto.update_one({'category_name':category['category_name']},
                           {"$set":{'promoted_patterns.0':category['seed_ctx_pattern']}})
        
def all_promoted_instances():
    instances_query = db_onto.find(projection={'promoted_instances':True, '_id':False})

    instances = list(flatten([list(d.values()) for d in list(instances_query)]))
        
    df_instances = pd.DataFrame(list(db_ap.find({"noun_phrase":{"$in":[i for i in instances]},
                                                 "counter":{"$gt":1}})))
    
    return df_instances

def all_promoted_patterns():
    patterns_query = db_onto.find(projection={'promoted_patterns':True, '_id':False})
    
    patterns = list(flatten([list(d.values()) for d in list(patterns_query)]))
    
    df_patterns = pd.DataFrame(list(db_ap.find({"ctx_pattern":{"$in":[p for p in patterns]},
                                                "counter":{"$gt":1}})))
    
    return df_patterns

def promote_instances(category, iteration, max_promotions ,limit, T, df_all_promoted_patterns):
    
    last_promoted_patterns = category['promoted_patterns'][iteration-1]
    promoted_instances = list(flatten(category['promoted_instances']))
    promoted_patterns = list(flatten(category['promoted_patterns']))

    #get the promoted patterns of the mutex exception categories
    mutex_query = (db_onto.
                   find({'category_name':{"$in":category['mutex_exceptions']}}))

    mutex_patterns = list(flatten([i['promoted_patterns'] for i in mutex_query]))

    #extraction step
    #count the ocurrences of instances that co-occur with the
    #positive promoted patterns in the last iteration
    #without considering instances that were already promoted
    pos = list(db_ap.find({"ctx_pattern":{"$in":last_promoted_patterns},
                           "noun_phrase":{"$nin":promoted_instances},
                           "counter":{"$gt":1}}))

    if (pos):  #if at least one positive and one negative pattern was found

        df_pos = (pd.DataFrame(pos)
                  .groupby('noun_phrase')
                  ['counter']
                  .sum()
                  .sort_values(ascending=False)
                  .head(limit)
                  .rename('count_pos'))

        #count the ocurrences of instances that co-occur with negative patterns
        df_neg = (df_all_promoted_patterns
                  [~df_all_promoted_patterns['ctx_pattern']
                   .isin(promoted_patterns + mutex_patterns)]
                  .groupby('noun_phrase')
                  ['counter']
                  .sum()
                  .rename('count_neg'))

        joined = (pd.concat([df_pos,df_neg], axis=1, sort=False)
                  .fillna(0)
                  .assign(filter_check=lambda df:  
                          (df['count_pos'] >= df['count_neg']*T) &  # filter criterion #1
                          (df['count_pos'] >= 2)))                  # filter criterion #2

        new_instances=list((joined[joined['filter_check']]                 # filter step
                            .sort_values(by='count_pos', ascending=False)  # rank step
                            .head(max_promotions)                          # promote step
                            .index.values))

        #update ontology with the promoted instances
        db_onto.update_one({'category_name':category['category_name']},
                           {'$set':{'promoted_instances.'+str(iteration):new_instances}})
    else:
        new_instances = []
    
    return new_instances
                  
def promote_patterns(category, iteration, max_promotions ,limit, T, df_all_promoted_instances):
    
    last_promoted_instances = category['promoted_instances'][iteration-1]
    promoted_patterns = list(flatten(category['promoted_patterns']))
    promoted_instances = list(flatten(category['promoted_instances']))

    #get the promoted instances of the mutex exception categories
    mutex_query = (db_onto.
                   find({'category_name':{"$in":category['mutex_exceptions']}}))

    mutex_instances = list(flatten([i['promoted_instances'] for i in mutex_query]))

    #extraction step
    #count the ocurrences of patterns that co-occur with the
    #positive promoted instances in the last iteration
    #without considering patterns that were already promoted
    pos = list(db_ap.find({"noun_phrase":{"$in":last_promoted_instances},
                           "ctx_pattern":{"$nin":promoted_patterns},
                           "counter":{"$gt":1}}))

    if (pos):  #if at least one positive and one negative instance was found
        df_pos = (pd.DataFrame(pos)
                  .groupby('ctx_pattern')
                  ['counter']
                  .sum()
                  .sort_values(ascending=False)
                  .head(limit)
                  .rename('count_pos'))

        #count the ocurrences of patterns that co-occur with negative instances
        df_neg = (df_all_promoted_instances
                  [~df_all_promoted_instances['noun_phrase']
                   .isin(promoted_instances + mutex_instances)]
                  .groupby('ctx_pattern')
                  ['counter']
                  .sum()
                  .rename('count_neg'))

        joined = (pd.concat([df_pos, df_neg], axis=1, sort=False)
                  .fillna(0)
                  .assign(filter_check=lambda df:  
                          (df['count_pos'] >= df['count_neg']*T) &   # filter criterion #1
                          (df['count_pos'] >= 2)))                   # filter criterion #2

        df_count = (df_all_promoted_instances
                    [df_all_promoted_instances['noun_phrase'].isin(promoted_instances)]
                    .groupby('ctx_pattern')
                    ['counter']
                    .sum()
                    .rename('count_cnt'))

        joined = (joined.join(df_count).fillna(0)
                  .assign(precision=lambda df:
                          df['count_pos']/df['count_cnt']))  #rank criterion

        new_patterns = list((joined[joined['filter_check']]                 # filter step
                             .sort_values(by='precision', ascending=False)  # rank step
                             .head(max_promotions)                          # promote step
                             .index.values))

        #update ontology with the promoted patterns
        db_onto.update_one({'category_name':category['category_name']},
                           {'$set':{'promoted_patterns.'+str(iteration):new_patterns}})

    else:
        new_patterns = []
        
    return new_patterns
    