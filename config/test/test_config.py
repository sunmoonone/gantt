# -*- coding: utf-8 -*-
'''
Created on Jul 9, 2015

@author: sunmoonone
'''
import unittest
import json
import yaml


class TestConfig(unittest.TestCase):

    def test_dump(self):
        pass
    
    def test_load(self):
        c="""
# domain and resource group mappings

mappings: 
    admin: 
        redis: &REDIS ["192.168.100.19": 6379]
        db: 
            master: [192.168.100.34, 3306, root, 123456]
            slave: [192.168.100.35, 3306, root, 123456]
        
        """
        with open('resource.yml','r') as f:
            d = yaml.load(f)
            print json.dumps(d, indent=2)
    

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()