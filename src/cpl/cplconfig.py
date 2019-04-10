import configparser
import os

class configCPL():

    def __init__(self):

        config = configparser.ConfigParser()
        config.read('../../conf/cpl.conf')

        self.db_address = config.get('MongoDB','db_address')
        
        self.num_iter = int(config.get('CPL','num_iter'))
        self.max_promotions = int(config.get('CPL','max_promotions'))
        self.limit = int(config.get('CPL','limit'))
        self.T = int(config.get('CPL','T'))