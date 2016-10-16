# -*- coding: utf-8 -*-
'''
A query class that operates on a single table

Created on Jul 27, 2015

@author: sunmoonone
'''
from common.orm.columns import _OrderByExpr,_OrExpr, _OrderByStatement,\
    _WhereStatement, _GroupByStatement, _ColList, _InsertStatement, _Table,\
    _SetStatement, _UpdateStatement, _DeleteStatement, _Limit, _SelectStatement,\
    _RawExpr, _Func, _From, _CreateTableStatement, _ColDefinitionList,\
    _ColDefinition, _KeyDefinition, _TableOption, _DropTableStatement,\
    _TruncateTableStatement, _InsertBySelectStatement, _ExplainStatement,\
    _IndexHintStatement, _IndexHint, INDEX_HINT_FORCE, INDEX_HINT_IGNORE,\
    INDEX_HINT_USE
from common.orm.errors import SaveNonDirty, EmptyResultSet

class Query(object):
    _TEST_=0
    
    def __init__(self,model,connection=None):
        self._model=model
        self._order=_OrderByStatement()
        self._where=_WhereStatement()
        self._group=_GroupByStatement()
        self._hints={}

        self._limit=None
        self._names=[]
        self._ex_names=[]

        self._conn=connection
    
    def _set_connection(self,conn):
        self._conn=conn
    
    
    def create_table(self,db=None):
        col_def_list=_ColDefinitionList()
        for c in self._model._cols_.values():
            col_def_list.add(_ColDefinition(c))

        for c in self._model._keys_.values():
            col_def_list.add(_KeyDefinition(c))

        stmt = _CreateTableStatement(_Table(self._model._table_,db), col_def_list, _TableOption(self._model._table_options_))

        if self._TEST_:return stmt

        return self._conn.execute(stmt.statement())
    
    def drop_table(self,db=None):
        stmt=_DropTableStatement(_Table(self._model._table_,db))

        if self._TEST_:return stmt

        return self._conn.execute(stmt.statement())
    
    def truncate_table(self,db=None):
        stmt=_TruncateTableStatement(_Table(self._model._table_,db))

        if self._TEST_:return stmt

        return self._conn.execute(stmt.statement())
    
    def insert(self,obj):
        '''
        @return tuple: (count,pkval)
        '''
        dirty = obj._get_dirty()
        if not dirty:
            raise SaveNonDirty('can not insert non-dirty object')

        vals=[c[1] for c in dirty]
        cols=_ColList( *(c[0] for c in dirty) )
        
        insert = _InsertStatement(_Table(self._model._table_), cols,vals)

        if self._TEST_:return insert

        c,_id = self._conn.execute(insert.statement(),vals)
        obj._set_pk(_id)
        return c

    def copy_to(self, model):
        '''copy data to table for `model`
        
        NOTE: only support copying between two models with the same columns
        '''
        cols = self._model._get_cols(self._names,self._ex_names)
        cols= _ColList( *cols )
        sel=self._build_select()

        insert = _InsertBySelectStatement(_Table(model._table_),cols,sel,sel.vals())

        if self._TEST_:return insert

        c,_ = self._conn.execute(insert.statement(),insert.vals())
        return c

#     def update_from(self,model, on):
    
    def save(self,obj, is_update=False):
        '''
        execute update if a primary key value found
        else execute insert
        '''
        dirty = obj._get_dirty()
        if not dirty:
            raise SaveNonDirty('can not save non-dirty object')

        try:
            pkcol,pkval = obj._get_pk()
        except:
            pkcol,pkval=None,None

        if pkval or is_update:
            if not pkval:
                uk=obj._get_uk()
                where = _WhereStatement(*[col==v for col,v in uk])
            else:
                where = _WhereStatement(pkcol==pkval)

            #execute update by pk
            set_stmt = _SetStatement()
            for col,v in dirty:
                if col.is_primary():
                    continue

                if not pkval and col.is_unique():
                    continue

                set_stmt.add(col.assign(v))

            update=_UpdateStatement(_Table(self._model._table_),set_stmt, where)


            if self._TEST_:return update 

            count,_= self._conn.execute(update.statement(), update.vals())
            return count
        
        #execute insert
        vals=[c[1] for c in dirty]
        insert=_InsertStatement(_Table(self._model._table_), _ColList(*(c[0] for c in dirty)), vals )

        if self._TEST_:return insert 

        count,_id = self._conn.execute(insert.statement(),vals)
        obj._set_pk(_id)
        return count
    
    def delete(self,obj=None):
        if obj:
            pkcol,pkval = obj._get_pk()
            delete=_DeleteStatement(_Table(self._model._table_), _WhereStatement(pkcol==pkval))
            return self._conn.execute(delete.statement(),delete.vals())
        
        #delete by query
        delete=_DeleteStatement(_Table(self._model._table_), self._where)

        if self._TEST_:return delete

        count,_  = self._conn.execute(delete.statement(),delete.vals())
        return count
    
    def update(self,*exprs,**values):
        '''
        update by query
        '''
        set_stmt=_SetStatement()
        for exp in exprs:
            set_stmt.add(exp)

        for k,v in values.items():
            col = self._model._get_col(k)
            set_stmt.add(col.assign(v))

        update = _UpdateStatement(_Table(self._model._table_), set_stmt, self._where)

        if self._TEST_:return update

        count,_ = self._conn.execute(update.statement(),update.vals())
        return count

    def filter(self,*exprs,**kwargs):
        self._where.add(*exprs)

        for k,v in kwargs.items():
            col = self._model._get_col(k)
            self._where.add(col==v)
        return self
    
    def order_by(self,*order_exprs):
        '''
        @param mixed order_expr: attribute name or _OrderByExpr object 
        '''
        for e in order_exprs:
            if isinstance(e, _OrderByExpr):
                self._order.append(e)
            else:
                e = self._model._get_col(e)
                self._order.append(e.asc())
        return self

    def group_by(self,*names):
        '''
        @param name mixed: attribute name or _Col object
        '''
        for n in names:
            col = self._model._get_col(n)
            self._group.add(col)
        return self
    
    def limit(self,offset,count):
        self._limit=_Limit(offset,count)
        return self
    
    def get(self,pk):
        pkcol = self._model._get_pk_col()
        self._where.append(pkcol==pk)
        return self.one()
    
    def one(self,as_dict=False):
        sel=self._build_select(limit=_Limit(0,1))
        ret = self._conn.execute(sel.statement(),sel.vals())
        if not ret.rows:
            return None
        
        if as_dict:
            return dict(zip(sel.col_names(),ret.rows[0]))
        return self._model._load_(sel.col_names(),ret.rows[0])
    
    def all(self, as_dict=False,as_scalar=False):
        sel=self._build_select()
        

        ret = self._conn.execute(sel.statement(),sel.vals())

        cols = sel.col_names()
        
        result=[]
        if ret:
            if as_scalar:
                for r in ret.rows:
                    result.append(r[0])
            elif as_dict:
                for r in ret.rows:
                    result.append(dict(zip(cols.r)))
            else:
                for r in ret.rows:
                    result.append( self._model._load_(cols,r) )

        return result

    def iter(self, as_dict=False,as_scalar=False):
        sel=self._build_select()

        ret = self._conn.execute(sel.statement(),sel.vals())
        cols = sel.col_names()
        
        if as_scalar:
            for r in ret.rows:
                yield r[0]
        elif as_dict:
            for r in ret.rows:
                yield dict(zip(cols,r))
        else:
            for r in ret.rows:
                yield self._model._load_(cols,r)
    
    def scalar(self):
        sel=self._build_select(limit=_Limit(0,1))
        ret = self._conn.execute(sel.statement(),sel.vals())
        if not ret.rows:
            return None
        return ret.rows[0][0]

    def count(self):
        sel=self._build_select(True,no_orderby=True)
        if self._TEST_:
            return sel

        sql = sel.statement()
        ret = self._conn.execute(sql,sel.vals())
        return ret.rows[0][0]

    def _build_select(self,count=False,no_orderby=False,limit=None):
        if count:
            return _SelectStatement(_ColList(_Func('count',_RawExpr('*'))), _From(self._model._table_, 
                                                                                  self._hints[self._model._table_] if self._model._table_ in self._hints else None), 
                                   where_stmt=self._where)
        else:
            return _SelectStatement(_ColList(*self._model._get_cols(self._names,self._ex_names)), 
                                   _From(self._model._table_,self._hints[self._model._table_] if self._model._table_ in self._hints else None), 
                                   where_stmt=self._where,
                                   groupby_stmt=self._group,
                                   orderby_stmt=None if no_orderby else self._order,
                                   limit_stmt= limit or self._limit)
    
    def get_select_statement(self):
        return self._build_select().statement()
    
    def only(self,*names):
        '''
        @param name: an attribute name or a _Col object
        '''
        names = self._model._assert_property(*names)
        self._names.extend(names)
        return self
    
    def exclude(self,*names):
        '''
        @param name: an attribute name or a column object
        '''
        names = self._model._assert_property(*names)
        self._ex_names.extend(names)
        return self

    def or_(self,*exprs):
        self._where.append(_OrExpr(*exprs))
        return self
    
    def has_appropriate_index(self):
        '''
        parse result of explain sql
        '''
        explain=_ExplainStatement(self._build_select())
        
        ret = self._conn.execute(explain.statement(),explain.vals())
        if not ret:
            raise EmptyResultSet('empty result for explain')

        ret=ret.rows[0]
        if ret[5] or 'Using index' in ret[-1]:
            return True
        return False
    
    def use_index(self,model, *index, **kw):
        '''
        @param model: model class
        @param index: object of Key or name of index 
        @kw: for_ = INDEX_HINT_FOR_JOIN | INDEX_HINT_FOR_ORDER_BY | INDEX_HINT_FOR_GROUP_BY . Default is None

        '''
        t = model._table_
        if t not in self._hints:
            self._hints[t]=_IndexHintStatement(t)

        self._hints[t].append(_IndexHint(INDEX_HINT_USE, (None if 'for_' not in kw else kw['for_']), *index))
        return self
    
    def force_index(self, model, *index, **kw):
        t = model._table_
        if t not in self._hints:
            self._hints[t]=_IndexHintStatement(t)

        self._hints[t].append(_IndexHint(INDEX_HINT_FORCE, (None if 'for_' not in kw else kw['for_']), *index))
        return self
    
    def ignore_index(self, model, *index, **kw):
        t = model._table_
        if t not in self._hints:
            self._hints[t]=_IndexHintStatement(t)

        self._hints[t].append(_IndexHint(INDEX_HINT_IGNORE, (None if 'for_' not in kw else kw['for_']), *index))
        return self
    
    
        
        

   
