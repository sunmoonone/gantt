# -*- coding: utf-8 -*-
'''
Created on Apr 7, 2016

@author: sunmoonone
'''
import unittest
from config import resource
from webtools.session import RedisCacheInterface, CacheStore
import time


class TestCacheStore(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        client = resource.SharedRedis()
        ci = RedisCacheInterface(client)
        cls.store = CacheStore(ci, 5, 'test_session')

    def test_CacheStore(self):
        self.store['a'] = 'foo'
        self.assertEqual('foo', self.store['a'])
        time.sleep(5)

        self.store['b'] = 'foo'
        self.assertEqual('foo', self.store['b'])

        self.store.cleanup(5)

        self.assertEqual('foo', self.store['b'])
        
        with self.assertRaises(KeyError):
            self.store['a']

        time.sleep(6)

        with self.assertRaises(KeyError):
            self.store['b']


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()