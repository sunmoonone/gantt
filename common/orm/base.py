# -*- coding: utf-8 -*-
'''
backend independent base model

Created on Jul 23, 2015

@author: sunmoonone
'''
from common.orm.query import Query
from common.orm.columns import _Col , Key
from common.orm.errors import *  # @UnusedWildImport
from collections import OrderedDict
from common.utils import ObjectDict

class _ModelMeta(type):
    def __new__(cls, name,base,dict_):
        if '__metaclass__' not in dict_:
            if '_table_' not in dict_ or not dict_['_table_']:
                raise OrmException('no attribute _table_ for model: %s' % name)
            
            if '_table_options_' in dict_:
                dict_['_table_options_'].update(BaseModel._table_options_)

            cls.parse_columns(dict_, name)

        return super(_ModelMeta,cls).__new__(cls, name, base, dict_)
    
    @classmethod
    def parse_columns(cls,dict_, theclass):
        may_keys=dict()
        icols=dict() # column name => column object
        keys=[]
        cols=[]
        for k in dict_:
            col = dict_[k]
            if isinstance(col,_Col):
                if not col.name:
                    col.name=k
                col._attr=k
                cols.append(col)
                
                if col.name in icols:
                    raise DuplicatedColumn(theclass,col.name)

                icols[col.name]=col
                
                if col.key:
                    may_keys[col.name]=col

            #is key
            elif isinstance(col,Key):
                if not col.name:
                    col.name=k
                col._attr=k
                keys.append(col)

        #sort by creation order
        cols.sort(cmp=cls._cmp_col)
        dcols=OrderedDict()
        for c in cols:
            dcols[c._attr]=c
            del c._order_
        del cols
        dict_['_cols_']=dcols

        for k in keys:
            k._parse_cols()

            ln=len(k.cols)    
            for n in k.cols:
                if n not in icols:
                    raise NoSuchColumn(theclass,n)

                if k.is_primary() or k.is_unique():
                    icols[n]._set_key_type(k.key_type)
                if ln==1:
                    if n in may_keys:
                        del may_keys[n]

        
        #sort by creation order
        keys.sort(cmp=cls._cmp_col)
        dkeys=OrderedDict()
        for c in keys:
            dkeys[c._attr]=c
            del c._attr
            del c._order_
        del keys

        #auto add key definitions to model
        for n,col in may_keys.items(): 
            k = Key('ak_%s' % n,col.key,(n,) )
            k._parse_cols()
            dkeys['ak_%s' % n] = k
        dict_['_keys_']=dkeys
        
    @classmethod
    def _cmp_col(cls,x,y):
        return cmp(x._order_, y._order_)

class BaseModel(object):

    _table_=''
    _table_options_={'engine':'InnoDB','auto_increment':1,'character set':'utf8','collate':'utf8_general_ci'}
    
    __metaclass__=_ModelMeta

    def __new__(cls, *args, **kwargs):
        obj = super(BaseModel,cls).__new__(cls, *args,**kwargs)
        obj._vals_={}
        for k in cls._cols_:
            obj._vals_[k] = None
        obj._dirty_={}
        return obj

    def __init__(self,**values):
        for k,v in values.items():
            self.__setattr__(k, v)
        
    @classmethod
    def _load_(cls,cols,row):
        #will not call __init__

        obj = cls.__new__(cls)
        cam = cls._col_attr_map()

        for k,v in zip(cols,row):
            a = cam[k]
            obj._vals_[a] = v
            if a in obj._dirty_:
                del obj._dirty_[a]
        return obj
            
    @classmethod
    def create_table(cls,conn,db=None):
        return Query(cls,conn).create_table(db)

    @classmethod
    def drop_table(cls,conn,db=None):
        return Query(cls,conn).drop_table(db)

    @classmethod
    def truncate_table(cls,conn,db=None):
        return Query(cls,conn).truncate_table(db)

    @classmethod
    def alter_table(cls,conn,db=None):
        raise NotImplementedError('alter_table')
    
    def save(self,conn=None):
        '''insert or update values in db
        '''
        return Query(self.__class__,conn).save(self)
    
    def insert(self,conn=None):
        '''insert values into db
        '''
        return Query(self.__class__,conn).insert(self)
    
    @classmethod
    def get(cls,pk):
        return Query(cls).get(pk)
    
    def update(self,conn=None):
        '''update values in db by pk
        '''
        return Query(self.__class__,conn).save(self,True)

    def delete(self,conn=None):
        '''delete values from db by pk
        '''
        return Query(self.__class__,conn).delete(self)
    
    @classmethod
    def filter(cls,*exprs):
        return Query(cls).filter(*exprs)

    @classmethod
    def objects(cls,conn=None):
        '''
        @return: Query
        '''
        return Query(cls,connection=conn)
    
    def __getitem__(self,name):
        return self.__getattribute__(name)

    def __setitem__(self,name,value):
        self.__setattr__(name, value)

    def __delitem__(self,name):
        self.__delattr__(name)

    def __getattribute__(self, name):
        cls = object.__getattribute__(self,'__class__')
        try:
            pr = object.__getattribute__(self,'_get_%s' % name)
        except:
            pr=None
        if pr:
            return pr()

        if name in cls._cols_:
            try:
                return self._vals_[name]
            except:
                raise AttributeError('%s.%s' % (self.__class__.__name__,name))
        

        return object.__getattribute__(self, name)

    def __setattr__(self,name,value):
        cls = object.__getattribute__(self,'__class__')
        try:
            pr = object.__getattribute__(self,'_set_%s' % name)
        except:
            pr=None

        if name not in cls._cols_:
            if pr:
                pr(name,value)
            else:
                object.__setattr__(self,name,value)
            return

        if name not in self._vals_:
            self._dirty_[name]=1
        elif self._vals_[name]!=value:
            self._dirty_[name]=1

        if pr:
            pr(name,value)
        else:
            self._vals_[name]=value
    
    def merge(self,values):
        '''merge values into local attributes
        '''
        for k in values:
            self.__setattr__(k,values[k])
    
    def __delattr__(self,name):
        if name in self._vals_:
            del self._vals_[name]
        if name in self._dirty_:
            del self._dirty_[name]
    
    def _get_uk(self):
        '''get col of unique key
        @return [(_Col, mixed), (_Col, mixed), ...]
        '''
        for k in self._keys_.values():
            if k.is_unique():
                names = k.cols.keys()
                cols = self._get_col_by_name(*names)
                if not isinstance(cols,list):
                    cols=(cols,)
                return [(col, self.__getattribute__(col._attr)) for col in cols]

        raise NoPrimaryKey('no unique key defined for model: %s' % self.__class__.__name__)

    def _get_pk(self):
        '''
        @return (_Col, Mixed)
        '''
        for k in self._keys_.values():
            if k.is_primary():
                names = k.cols.keys()
                col = self._get_col_by_name(names[0])
                return col, self.__getattribute__(col._attr)

        raise NoPrimaryKey('no primary key defined for model: %s' % self.__class__.__name__)
    
    def _set_pk(self,val):
        for c in self._cols_:
            if self._cols_[c].is_primary():
                if not self[c]:
                    self.__setattr__(c, val)

    @classmethod
    def _get_pk_col(cls):
        for k in cls._keys_.values():
            if k.is_primary():
                names = k.cols.keys()
                return cls._get_col_by_name(names[0])

#         for c in cls._cols_:
#             if cls._cols_[c].key==Key.KEY_PRIMARY:
#                 return cls._cols_[c]
        raise NoPrimaryKey('no primary key defined for model:%s' % cls.__name__)

    def _get_dirty(self):
        '''
        @return list: [(_Col_object,mixed_value),...]
        '''
        if not self._dirty_:
            return None
        return [(self._cols_[k], self.__getattribute__(k)) for k in self._dirty_]

    @classmethod
    def _assert_property(cls,*names):
        cam=cls._col_attr_map()
        ret=[]
        for n in names:
            if isinstance(n, _Col):
                if n.name not in cam:
                    raise NoSuchColumn(cls.__name__,n.name)
                else:
                    ret.append(cam[n.name])

            elif n not in cls._cols_:
                raise NoSuchAttribute(cls.__name__,n)
            else:
                ret.append(n)
        return ret

    @classmethod
    def _col_attr_map(cls):
        d={}
        for k,v in cls._cols_.items():
            d[v.name]=k
        return d

    @classmethod
    def _get_cols(cls,only_attrs=None,ex_attrs=None):
        '''
        @return list: a list of _Col objects
        '''
        if only_attrs:
            return (v for k,v in cls._cols_.items() if k in only_attrs)
        elif ex_attrs:
            return (v for k,v in cls._cols_.items() if k not in ex_attrs)
        return cls._cols_.values()
    
    @classmethod
    def _get_col_names(cls,only_attrs=None,ex_attrs=None):
        '''
        @return list: a list of column names
        '''
        return [c.name for c in cls._get_cols(only_attrs, ex_attrs)]

    @classmethod
    def _get_col_by_name(cls,*names):
        '''
        @return list or _Col
        '''
        ret=[]
        for n in names:
            for c in cls._cols_.values():
                if c.name==n:
                    ret.append(c)
        if not ret:
            return None

        if len(names)>1:
            return ret

        return ret[0]
            

    @classmethod
    def _get_col(cls,attr):
        '''
        @return _Col: a _Col object
        '''
        if isinstance(attr, _Col):
            cam=cls._col_attr_map()
            if not attr.name in cam:
                raise NoSuchColumn(cls.__name__,attr.name)
            return attr
        elif attr not in cls._cols_:
            raise NoSuchAttribute(cls.__name__,attr)
        return cls._cols_[attr]
               
    def to_dict(self,*only,**kw):
        return self._dict(*only,**kw)
    
    def to_odict(self,*only,**kw):
        return ObjectDict(self._dict(*only,**kw))
            
    def _dict(self, *only,**kw):
        if self._vals_:
            ex=kw['exclude'] if 'exclude' in kw and kw['exclude'] else None

            d = self._vals_.copy()
            if only or ex:
                for k in d.keys():
                    if ex and k in ex:
                        del d[k]
                        continue
                    if only and k not in only:
                        del d[k]
            return d
        return {}

    def clone(self,*only):
        if only:
            vals=dict()
            for k in only:
                if k in self._vals_:
                    vals[k]=self._vals_[k]
                else:
                    vals[k]=None
            obj=self.__class__(**vals)
        else:
            obj = self.__class__(**self._vals_)
        return obj
            
