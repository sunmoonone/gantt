# -*- coding: utf-8 -*-
'''
Created on Jan 7, 2016

@author: sunmoonone
'''
import unittest
from config import resource
import logging


class TestResource(unittest.TestCase):

    def testRoot(self):
        logger = logging.root
        logger.debug('root says hello world')
        logger.info('root says hello world')
        logging.debug('logging syas hello world')
        print logger.handlers
    
    def test_1logging(self):
        l=logging.getLogger('m')
        l.debug('hello')
        l.warn('warning hello')
        print 'l.handler',l.handlers
        
        mn=logging.getLogger('m.n')
        print 'mn.handler',mn.handlers
        print 'mn',logging.getLevelName(mn.level)
        mn.warn('warning hello')

    def test_2Logger(self):
        a=resource.Logger('a')
        print 'a.handlers',a.handlers
        
        l2=resource.Logger('a.b', 'b')
        print 'l2.handlers',l2.handlers
        print 'l2.clone',l2.clone
        l3=resource.Logger('a.b', 'b')
        print 'l3.handler',l3.handlers
        print 'l3.clone',l3.clone
        print 'l2 is l3',l2 is l3

        b=resource.Logger('b')
        print 'b.handler',b.handlers
        print 'b',logging.getLevelName(b.level)
        b.debug('warning hello')
        b.warn('warning hello')

        l4=resource.Logger('a.c')
        print 'l4.handlers',l4.handlers
        print 'l4.clone',l4.clone
        
        l4.debug('hello world')
        l4.info('you')
        
        l5=l4.clone('a.d')
        print 'l5.handlers',l5.handlers
        print 'l5.clone',l5.clone
        l5.debug('hello world')
        l5.info('you')
        
        
        mn=resource.Logger('m.n','m.n')
        print 'mn.handler',mn.handlers
        print 'mn',logging.getLevelName(mn.level)
        mn.warn('warning hello')

        mo=resource.Logger('m.o','o','mo')
        print 'mo.handler',mo.handlers
        print 'mo',logging.getLevelName(mo.level)
        mo.debug('hello')
        mo.info('hello')
        mo.warn('hello')


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLogger']
    unittest.main()