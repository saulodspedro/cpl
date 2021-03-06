import configparser
import os
import sys

import os 
dir_path = os.path.dirname(os.path.realpath(__file__))
print(dir_path)

class configCPL():

    def __init__(self):

        config = configparser.ConfigParser()
        config.read('../../conf/cpl.conf')

        self.db_address = config.get('MongoDB','db_address')
        
        self.num_iter = int(config.get('CPL','num_iter'))
        self.max_p_promotions = int(config.get('CPL','max_p_promotions'))
        self.max_i_promotions = int(config.get('CPL','max_i_promotions'))
        self.limit = int(config.get('CPL','limit'))
        self.T = int(config.get('CPL','T'))
