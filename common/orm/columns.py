# -*- coding: utf-8 -*-
'''
A mysql implementation

Created on Jul 27, 2015

@author: sunmoonone
'''
from common.orm.errors import OrmException, KeyDefinitionError
from abc import abstractmethod, ABCMeta
from collections import OrderedDict

def stuple(iterable):
    return tuple(str(e) for e in iterable) 
    
class _Expr(object):
    __metaclass__=ABCMeta

    @abstractmethod
    def statement(self):
        '''
        @return string: a prepared statement
        '''

    def vals(self):
        '''
        @return list: always a list
        '''
        return []

    @classmethod
    def _statement(cls,obj):
        if isinstance(obj, _Expr):
            return obj.statement()
        return '%s'
            
class _EmptyExpr(_Expr):

    def statement(self):
        return ''

class _RawExpr(_Expr):
    def __init__(self,raw):
        self.raw=raw

    def statement(self):
        return str(self.raw)

class _BinaryExpr(_Expr):

    def __init__(self,left,operator,right):
        self._oper=operator
        self._left=left
        self._right=right

    def statement(self):
        return ' '.join(( self._statement(self._left),self._oper,self._statement(self._right) ))
    
    def _get_oper(self):
        return self._oper
    operator=property(fget=_get_oper)
    
    def _get_operand(self):
        return [self._left,self._right]
    operand=property(fget=_get_operand)
    
    def vals(self):
        vals=[]
        
        if not isinstance(self._left, _Expr):
            vals.append(self._left)
        else:
            vals.extend(self._left.vals())

        if callable(self._right):
            vals.append(self._right())
        elif isinstance(self._right, _Expr):
            vals.extend(self._right.vals())
        else:
            vals.append(self._right)

        return vals
    
class _InExpr(_BinaryExpr):
    def statement(self):
        ps=[ self._statement(self._left),self._oper, '(']
        if isinstance(self._right, _Expr):
            ps.append(self._right.statement())
        else:
            ps.append(','.join(['%s']*len(self._right)))
        ps.append(')')
        return ' '.join(ps)

    def vals(self):
        vals=[]
        
        if not isinstance(self._left, _Expr):
            vals.append(self._left)
        else:
            vals.extend(self._left.vals())

        if isinstance(self._right, _Expr):
            vals.extend(self._right.vals())
        else:
            vals.extend(self._right)
            
        return vals

class _UnaryExpr(_Expr):
    '''
    unary operator
    '''
    def __init__(self,operator,operand,right=False):
        '''
        @param operator: a string 
        @param mixed operand: any object 
        @param right: operator at right, default at left
        '''
        self._oper=operator
        self._operand=operand
        self._right=right

    def _get_oper(self):
        return self._oper
    operator=property(fget=_get_oper)
    
    def _get_operand(self):
        return self._operand
    operand=property(fget=_get_operand)
    
    def statement(self):
        if self._right:
            return ' '.join(( self._statement(self._operand),self._oper ))
        return ' '.join(( self.oper,self._statement(self._operand) ))
    
    def vals(self):
        if not isinstance(self._operand, _Expr):
            return [self._operand]
        else:
            return self._operand.vals()

class _OrderByExpr(_Expr):
    def __init__(self,col,direction='asc'):
        '''
        @param col: a _Col object 
        '''
        self.col=col
        self.direction=direction

    def statement(self):
        return ' '.join((self.col.statement(), self.direction))

class _SizeExpr(_Expr):
    def __init__(self,col,size):
        self.col=col
        self.size=size
    
    def statement(self):
        return ''.join( (self.col.statement(),' (',str(self.size),')') )
    
    def asc(self):
        return _OrderByExpr(self,'asc')
    
    def desc(self):
        return _OrderByExpr(self,'desc')

class _OrderByStatement(_Expr):
    def __init__(self,*orderby_exprs):
        self._exprs=list(orderby_exprs)

    def add(self,*orderby_expr):
        self._exprs.extend(orderby_expr)

    def append(self,*orderby_expr):
        self._exprs.extend(orderby_expr)

    def statement(self):
        if not self._exprs:return ''
        
        return ''.join(('order by ',','.join((e.statement() for e in self._exprs))))

class _GroupByStatement(_Expr):
    def __init__(self,*col):
        '''
        @param col: a _Col object 
        '''
        self._cols=list(col)

    def add(self,*col):
        '''
        @param col: a _Col object 
        '''
        self._cols.extend(col)
        
    def append(self,*col):
        self._cols.extend(col)

    def statement(self):
        if not self._cols:return ''
        return ''.join( ('group by ', ','.join((e.statement() for e in self._cols))) )

INDEX_HINT_USE=0
INDEX_HINT_FORCE=1
INDEX_HINT_IGNORE=2

INDEX_HINT_FOR_JOIN=0
INDEX_HINT_FOR_ORDER_BY=1
INDEX_HINT_FOR_GROUP_BY=2

class _IndexHint(_Expr):
    def __init__(self,type_=INDEX_HINT_USE, for_=None, *index):
        self._type=type_
        self._for=for_
        self._idx_list=list(index)
    
    def statement(self):
        
        return ' '.join(( 'USE' if self._type==INDEX_HINT_USE else 'IGNORE' if self._type==INDEX_HINT_IGNORE else 'FORCE', 'INDEX',
                         '' if not self._for else 'FOR JOIN' if self._for==INDEX_HINT_FOR_JOIN else 'FOR ORDER BY' if self._for==INDEX_HINT_FOR_ORDER_BY else 'FOR GROUP BY',
                         '(',','.join([self._nameof(i) for i in self._idx_list]),')'
                             ))

    @classmethod
    def _nameof(cls,obj):
        if isinstance(obj, _Expr):
            return obj.name
        return str(obj)
            

class _IndexHintStatement(_Expr):
    def __init__(self, table=None, *hints):
        self._table=table
        self._hints=list(hints)
    
    def append(self, hint):
        self._hints.append(hint)
    
    def statement(self):
        return ' '.join([h.statement() for h in self._hints])

class _Limit(_Expr):
    def __init__(self,offset,count):
        self._offset=int(offset)
        self._count=int(count)
    
    def statement(self):
        return 'limit %s,%s'

    def vals(self):
        return [self._offset,self._count]

class _Table(_Expr):
    def __init__(self,name,db=None):
        self._name=name
        self._db=db
    
    def statement(self):
        if self._db:
            return '`%s`.`%s`' % (self._db,self._name)
        return '`%s`' % self._name

class _Database(_Table):
    def __init__(self,name):
        self._name=name
    
    def statement(self):
        return '`%s`' % self._name

class _From(_Expr):
    def __init__(self,table, index_hint_stmt=None):
        '''
        @param table: a string or a _Table object
        '''
        if not isinstance(table, _Expr):
            self._table=_Table(table)
        else:
            self._table=table
        self._index_hint_stmt=index_hint_stmt
    
    def statement(self):
        if self._index_hint_stmt:
            return 'from %s %s' % (self._table.statement(), self._index_hint_stmt.statement())
        return 'from %s' % self._table.statement()

class _ColList(_Expr):
    def __init__(self,*col):
        '''
        @param col: a _Col object  or a col_expr
        '''
        self._cols=col

    def statement(self):
        return ','.join( (c.statement() for c in self._cols) )
    
    def col_names(self):
        return [col.name for col in self._cols]

class _Func(_Expr):
    def __init__(self,name,*args):
        self.name=name
        self.args=args

    def statement(self):
        return ''.join( (self.name,'(', ','.join( (self._statement(e) for e in self.args) ) ,')'))
    
    def vals(self):
        vals=[]
        for e in self.args:
            if isinstance(e, _Expr):
                vals.extend(e.vals())
            else:
                vals.append(e)

class _SetStatement(_Expr):
    def __init__(self,*expr):
        '''
        @param expr: a _BinaryExpr object with a `=` operator
        '''
        self._exprs=list(expr)
    
    def add(self,*expr):
        '''
        @param expr: a _BinaryExpr object with a `=` operator
        '''
        self._exprs.extend(expr)

    def statement(self):
        return ''.join(('set ',','.join( (e.statement() for e in self._exprs) ) ))

    def vals(self):
        vals=[]
        for e in self._exprs:
            vals.extend(e.vals())
        return vals

class _WhereStatement(_Expr):
    def __init__(self,*expr):
        '''
        @param expr: a _BinaryExpr or _UnaryExpr object 
        '''
        self._exprs=list(expr)
    
    def add(self,*expr):
        '''
        @param expr: a _BinaryExpr or _UnaryExpr object 
        '''
        for e in expr:
            if not isinstance(e, _Expr):
                raise OrmException('invalid where expression:%s' % e)
        self._exprs.extend(expr)

    def append(self,*expr):
        self._exprs.extend(expr)
    
    def statement(self):
        if not self._exprs:
            return ''
        return ''.join(('where ',' and '.join((e.statement() for e in self._exprs)) ))

    def vals(self):
        vals=[]
        for e in self._exprs:
            vals.extend(e.vals())
        return vals

class _OrExpr(_Expr):
    def __init__(self,*exprs):
        if len(exprs)<2:
            raise OrmException('or expression requires at least 2 _Expr objects')
        self._exprs=exprs
    
    def statement(self):
        return ''.join( ('(',' or '.join((e.statement() for e in self._exprs)),')') )
        
    def vals(self):
        vals=[]
        for e in self._exprs:
            vals.extend(e.vals())
        return vals

class _SelectStatement(_Expr):

    def __init__(self,col_list_expr,from_stmt=None,where_stmt=None,groupby_stmt=None,orderby_stmt=None,limit_stmt=None):
        empty = _EmptyExpr()
        self.col_list_expr=col_list_expr
        self.from_stmt=from_stmt or empty
        self.where_stmt=where_stmt or empty
        self.groupby_stmt=groupby_stmt or empty
        self.orderby_stmt=orderby_stmt or empty
        self.limit_stmt=limit_stmt or empty
    
    def statement(self):
        return ' '.join(('select ',self.col_list_expr.statement(),self.from_stmt.statement(),
                        self.where_stmt.statement(), self.groupby_stmt.statement(),self.orderby_stmt.statement(),
                        self.limit_stmt.statement()))
    def vals(self):
        vals=[]
        vals.extend(self.col_list_expr.vals())
        vals.extend(self.where_stmt.vals())
        vals.extend(self.limit_stmt.vals())
        return vals

    def col_names(self):
        return self.col_list_expr.col_names()

class _ExplainStatement(_Expr):
    def __init__(self,select_stmt):
        self._sel=select_stmt
    
    def statement(self):
        return 'explain %s' % self._sel.statement()
    
    def vals(self):
        return self._sel.vals()

class _UpdateStatement(_Expr):
    def __init__(self,table_expr,set_stmt,where_stmt=None):
        self.table_expr=table_expr
        self.set_stmt=set_stmt
        self.where_stmt=where_stmt or _EmptyExpr()
    
    def statement(self):
        return ' '.join(('update ',self.table_expr.statement(),self.set_stmt.statement(), self.where_stmt.statement()))

    def vals(self):
        vals=[]
        vals.extend(self.set_stmt.vals())
        vals.extend(self.where_stmt.vals())
        return vals

class _DeleteStatement(_Expr):
    def __init__(self,table_expr,where_stmt=None):
        self.table_expr=table_expr
        self.where_stmt=where_stmt or _EmptyExpr()
    
    def statement(self):
        return ' '.join(('delete from ',self.table_expr.statement(),self.where_stmt.statement()))

    def vals(self):
        return self.where_stmt.vals()

class _InsertStatement(_Expr):
    def __init__(self,table_expr,col_list_expr,value_list=[]):
        self.table_expr=table_expr
        self.col_list_expr=col_list_expr
        self.value_list=value_list
        
    def statement(self):
        return ' '.join(('insert into ',self.table_expr.statement(),'(',self.col_list_expr.statement(),')',
                         'values(',','.join(('%s' for _ in self.value_list)),')'))
    def vals(self):
        return self.value_list

class _InsertBySelectStatement(_Expr):
    def __init__(self,table_expr, col_list_expr, select_statement, value_list=[]):
        self.table_expr=table_expr
        self.col_list_expr=col_list_expr
        self.select=select_statement
        self.value_list=value_list
        
    def statement(self):
        return ' '.join(('insert into ',self.table_expr.statement(),
                         '(',self.col_list_expr.statement(),')',
                         self.select.statement()))
    def vals(self):
        return self.value_list


class _ColDefinition(_Expr):
    def __init__(self,col):
        '''
        @param col: a _Col object 
        '''
        self._col=col
    
    def statement(self):
        return self._col.definition()

class _KeyDefinition(_Expr):
    def __init__(self,key):
        '''
        @param key: a Key object 
        '''
        self._key=key

    def statement(self):
        return self._key.definition()

class _ColDefinitionList(_Expr):
    def __init__(self,*col_def_expr):
        self.df=list(col_def_expr)

    def add(self,*df):
        self.df.extend(df)
    
    def statement(self):
        return ',\n'.join((e.statement() for e in self.df))

class _TableOption(_Expr):
    def __init__(self,options):
        self._opt=options
    
    def statement(self):
        return ' '.join(( '%s=%s' % (k,v) for k,v in self._opt.items() ))
    
class _CreateTableStatement(_Expr):
    def __init__(self, table_expr,col_def_list_expr,table_option_expr=None,partition_expr=None):
        empty = _EmptyExpr()
        self.table_expr=table_expr
        self.col_def_list=col_def_list_expr
        self.table_option=table_option_expr or empty
        self.partition_option=partition_expr or empty
    
    def statement(self):
        return ''.join( ('create table if not exists ', self.table_expr.statement(),'(', 
                         self.col_def_list.statement(),')',self.table_option.statement(), self.partition_option.statement() ) )
    
class _DropTableStatement(_Expr):
    def __init__(self,table_expr):
        self.table_expr=table_expr
        
    def statement(self):
        return ''.join(('drop table ',self.table_expr.statement()))
       
class _TruncateTableStatement(_DropTableStatement): 
    def statement(self):
        return ''.join(('truncate table ',self.table_expr.statement()))

def head_till_any(s,chars):
    i=0
    for c in s:
        if c in chars:
            break
        i+=1
    return s[:i].strip('`')
    
class Key(_Expr):
    __counter__=0
    KEY_PRIMARY=1
    KEY_UNIQUE=2
    KEY_FOREIGN=3
    KEY_NORMAL=4

    def __init__(self,name='',type_=KEY_NORMAL,cols=(),storage=None): 
        '''
        @param name:string a index name
        @param key_type: int 
        @param cols mixed(str|tuple|dict): a tuple of string or a dictionary with column name as key, integer length as value 
        @param storage:string btree|hash
        '''
        self.name=name
        self.key_type=type_

        #deferred to parse
        self.cols=cols

        self.storage=storage
        self._order_=Key.__counter__
        Key.__counter__+=1
    
    def _parse_cols(self):
        cols=self.cols
        self.cols=OrderedDict()
        if isinstance(cols, (basestring,_Expr)):
            cols=(cols,)

        if isinstance(cols, (tuple,list)):
            for c in cols:
                if isinstance(c,basestring):
                    self.cols[c]=c
                elif isinstance(c, _Col):
                    self.cols[c.name]=c.statement()
                elif isinstance(c,_Expr):
                    exp=c.statement()
                    self.cols[head_till_any(exp,' (')]=exp
                else:
                    raise KeyDefinitionError('invalid column: %s' % c)

        else:
            raise KeyDefinitionError('invalid argument cols')
        
        if not self.cols:
            raise KeyDefinitionError('invalid argument cols')

    def is_primary(self):
        return self.key_type==self.KEY_PRIMARY
    
    def is_unique(self):
        return self.key_type==self.KEY_UNIQUE
    
    def statement(self):
        return '`%s`' % self.name
    
    def type_name(self):
        if self.key_type==self.KEY_PRIMARY:
            return 'primary key'
        elif self.key_type==self.KEY_FOREIGN:
            return 'foreign key'
        elif self.key_type==self.KEY_UNIQUE:
            return 'unique key'
        elif self.key_type==self.KEY_NORMAL:
            return 'index'

    def definition(self):
        return ' '.join(( self.type_name(), self.statement() if self.key_type!=self.KEY_PRIMARY else '',
                          'using %s' % self.storage if self.storage else '',
                          '(', ','.join(self.cols.values()),')',
                           ))
            
        
class _Col(_Expr):
    __counter__=0

    def __init__(self,name='',size=(),unsigned=False,null=True,primary=False,unique=False,key=False,auto_inc=False,default=None,comment=None):
        self._attr=None # automatically set when class is builded
        self.name=name
        if not isinstance(size, (tuple,list)):
            self.size=tuple((size,))
        else:
            self.size=tuple(size)
        self.nullable=null
        self.key=0
        self.auto_inc=auto_inc
        
        self.unsigned=unsigned

        if primary:
            self.key=Key.KEY_PRIMARY
        elif unique:
            self.key=Key.KEY_UNIQUE
        elif key:
            self.key=Key.KEY_NORMAL

        self.default=default
        self.comment=comment
        self._order_=_Col.__counter__
        _Col.__counter__=_Col.__counter__+1

    def _set_key_type(self,val):
        self.key = val

    def is_primary(self):
        return self.key==Key.KEY_PRIMARY
    
    def is_unique(self):
        return self.key==Key.KEY_UNIQUE

    def statement(self):
        return '`%s`' % self.name
    
    def __str__(self):
        return self.name
        

    def definition(self):
        return ' '.join((  self.statement(),self._type_, ''.join(('(',','.join(stuple(self.size)),')')) if self.size else '',
                           'unsigned' if self.unsigned else '',
                           'null' if self.nullable else 'not null', 
                           "default '%s'" % self.default if self.default!=None else '',
                           'auto_increment' if self.auto_inc else '',
                           "comment '%s'" % self.comment if self.comment else ''
                             ))

    def vals(self):
        return []
    
    def __gt__(self,other):
        return _BinaryExpr(self,'>',other)

    def __lt__(self,other):
        return _BinaryExpr(self,'<',other)

    def __ge__(self,other):
        return _BinaryExpr(self,'>=',other)

    def __gte__(self,other):
        return _BinaryExpr(self,'>=',other)

    def __le__(self,other):
        return _BinaryExpr(self,'<=',other)

    def __lte__(self,other):
        return _BinaryExpr(self,'<=',other)

    def __eq__(self,other):
        return _BinaryExpr(self,'=',other)

    def __ne__(self,other):
        return _BinaryExpr(self,'!=',other)
    
    def __add__(self,other):
        return _BinaryExpr(self,'+',other)
    
    def __sub__(self,other):
        return _BinaryExpr(self,'-',other)

    def __neg__(self):
        return _UnaryExpr('-',self)
    
    def __mul__(self,other):
        return _BinaryExpr(self,'*',other)
    
    def __div__(self,other):
        return _BinaryExpr(self,'/',other)
    
    def __and__(self,other):
        return _BinaryExpr(self,'&',other)
    
    def __or__(self,other):
        return _BinaryExpr(self,'|',other)
    
    def __xor__(self,other):
        return _BinaryExpr(self,'^',other)
    
    def asc(self):
        return _OrderByExpr(self,'asc')
    
    def desc(self):
        return _OrderByExpr(self,'desc')
    
    def is_null(self):
        return _UnaryExpr('is null' ,self, True)

    def not_null(self):
        return _UnaryExpr('is not null' ,True)
    
    def like(self,other):
        '''
        @param other:string a pattern
        '''
        return _BinaryExpr(self,'like',other)

    def not_like(self,other):
        '''
        @param other:string a pattern
        '''
        return _BinaryExpr(self,'not like',other)

    def contains(self,value):
        return _BinaryExpr(self,'like','%%%s%%' % value)

    def not_contains(self,value):
        return _BinaryExpr(self,'not like','%%%s%%' % value)

    def startswith(self,value):
        return _BinaryExpr(self,'like','%s%%' % value)

    def endswith(self,value):
        return _BinaryExpr(self,'like','%%%s' % value)
    
    def in_(self,lst):
        if not isinstance(lst, (list,tuple)):
            raise Exception('invalid operand for sql in')
        return _InExpr(self,'in',lst)
    
    def not_in_(self,lst):
        return _InExpr(self,'not in',lst)

    def assign(self,other):
        return _BinaryExpr(self,'=',other)

    def __call__(self,size):
        return _SizeExpr(self,size)

class ColInt(_Col):
    _type_='int'

class ColChar(_Col):
    _type_='char'

class ColText(_Col):
    _type_='text'

class ColMediumText(_Col):
    _type_='mediumtext'

class ColLongText(_Col):
    _type_='longtext'

class ColTinyText(_Col):
    _type_='tinytext'

class ColTinyInt(_Col):
    _type_='tinyint'

class ColBigInt(_Col):
    _type_='bigint'

class ColFloat(_Col):
    _type_='float'

class ColDouble(_Col):
    _type_='double'

class ColDecimal(_Col):
    _type_='decimal'

class ColDatetime(_Col):
    _type_='datetime'

class ColBit(_Col):
    _type_='bit'

class ColBinary(_Col):
    _type_='binary'

class ColVarBinary(_Col):
    _type_='varbinary'

class ColString(_Col):
    _type_='varchar'


