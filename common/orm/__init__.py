'''
a simple but enough orm lib

@author: sunmoonone
'''
__all__=['BaseModel','Query','ColBigInt','ColInt','ColBinary','ColBit','ColChar','ColDatetime','ColDecimal','ColDouble',
         'ColFloat','ColString','ColText','ColTinyInt','ColVarBinary','Key','ColLongText','ColMediumText','ColTinyText']

from common.orm.base import BaseModel
from common.orm.columns import ColBigInt,ColInt,ColBinary,ColBit,ColChar,ColDatetime,ColDecimal,ColDouble,ColFloat,ColString,ColText,ColTinyInt,ColVarBinary,Key,ColLongText,ColMediumText,ColTinyText
from common.orm.query import Query