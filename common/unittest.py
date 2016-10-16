# -*- coding: utf-8 -*-
'''
Created on Dec 29, 2015

@author: sunmoonone
'''

import unittest
from common import errors

errors.DISPLAY_TRACE=True

class ControllerBaseCase(unittest.TestCase):

    def assertState(self,res,state):
        if res.normal_body.find('"state": %s' % state)==-1:
            print res.normal_body
            raise self.failureException('Response state is not %s' % state)
    
    def assertException(self,res,name):
        if res.normal_body.find(name)==-1:
            print res.normal_body
            raise self.failureException('No Traceback for %s' % name)
    



