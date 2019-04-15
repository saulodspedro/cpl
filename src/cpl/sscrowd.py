import pandas as pd
from cpl.utils import db_res

def sscrowd_scores():
    columns = ['category','ctx_pattern','score']
    
    df_sscrowd = pd.DataFrame(db_res.find()).set_index('ctx_pattern')
    
    return df_sscrowd