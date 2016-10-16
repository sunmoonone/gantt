# -*- coding: utf-8 -*-
'''
Created on Jul 27, 2015

@author: sunmoonone
'''

class OrmException(Exception):
    pass
 
class KeyDefinitionError(OrmException):
    pass

class NoPrimaryKey(OrmException):
    pass

class NoUniqueKey(OrmException):
    pass

class NoSuchAttribute(OrmException):
    
    def __init__(self, model,name):
        super(NoSuchAttribute,self).__init__('no such attribute %s.%s' % (model,name))

class NoSuchColumn(OrmException):
    def __init__(self, model, name):
        super(NoSuchColumn,self).__init__('no such column %s.%s' % (model,name))

class DuplicatedColumn (OrmException):
    def __init__(self, model, name):
        super(DuplicatedColumn,self).__init__('duplicated column %s.%s' % (model,name))

class SaveNonDirty(OrmException):
    pass

class EmptyResultSet(OrmException):
    pass