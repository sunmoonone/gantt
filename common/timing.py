# -*- coding: utf-8 -*-
'''
Created on Jan 2, 2016

@author: sunmoonone
'''
from common.threadlocal import ThreadingLocal
import time
from collections import OrderedDict
from config import settings

Enabled=settings.profile.enabled

class TimingError(Exception):
    pass

class SimpleTimer(object):
    def __init__(self,start=False):
        if start:
            self.start()
    
    def mark(self):
        if not Enabled:return

        self._start=time.time()
        return self._start
    
    def span(self):
        '''
        @return: time span in milliseconds
        '''
        if not Enabled:return

        return round( (time.time()-self._start)*1000,3)

class Timer(object):
    ''' A thread-safe timer for calculating processing time
    
    
    example:
    
    Timer.mark('executing')
    # code
    # code
    Timer.end('executing')
    
    for label, timespan in Timer.times():
        print label,'spent',timespan,'ms'
        
    
    #get time spent of label
    timespan = Timer.span('executing')
    print 'executing spent',timespan,'ms'
        
    '''

    _marks=ThreadingLocal()
    
    @classmethod
    def _patch(cls): 
        if 'starts' not in cls._marks:
            cls._marks.starts={}
        if 'times' not in cls._marks:
            cls._marks.times=OrderedDict()

    @classmethod
    def mark(cls,label=None, *args):
        '''mark start time for label
        @param label: mark name
        @param args:  information for this label
        '''
        if not Enabled:return

        cls._patch()
        t=time.time()
        cls._marks.starts[label]=t
        cls._marks.starts['%s___args_' % label]=args
        return t

    @classmethod
    def get_args(cls,label):
        if 'starts' not in cls._marks:
            return None

        k= '%s___args_' % label
        if k in cls._marks.starts:
            return cls._marks.starts[k]
    
    
    @classmethod
    def end(cls, label, *args):
        '''End label and store time span for this `label` to _ends
        @param label:  mark to end
        @param another: [optional] another label whose time should be subtracted from this label
        @param ignore_another: [optional] useful when another labels' time is 0
        '''
        if not Enabled:return

        cls._patch()
        if label not in cls._marks.starts:
            raise TimingError('Unknown label: %s' % label)

        if args:
            m=args[0]
            if m in cls._marks.times :
                cls._marks.times[label] = round((time.time() - cls._marks.starts[label])*1000 - cls._marks.times[m], 3)

            elif len(args)==1 or not args[1]:
                raise TimingError('Unknown label to subtract: %s' % m)
            else:
                cls._marks.times[label] = round((time.time() - cls._marks.starts[label])*1000,3)
                
        else:
            cls._marks.times[label] = round((time.time() - cls._marks.starts[label])*1000,3)

        del cls._marks.starts[label]
    
    @classmethod
    def span(cls,label=None):
        '''
        @return: time span in milliseconds
        '''
        if not Enabled:return

        cls._patch()
        if label in cls._marks.starts:
            return round((time.time() - cls._marks.starts[label])*1000,3)
        else:
            raise TimingError('Unknown label: %s' % label)

    @classmethod
    def clear(cls):
        cls._marks.__clear__()
        
    @classmethod
    def times(cls):
        if not Enabled:return

        cls._patch()
        return ((k,v) for k,v in cls._marks.times.items())


def mark_time(*ags):
    '''
    @mark_time
    def foo():
        ...

    @mark_time('label')
    def foo1():
        ...

    @mark_time('this label','another label whose time will be subtracted from `this label`')
    def foo():
        foo1()
        ...

    '''
    func,marks={},[]

    def timing(*args,**kargs):
        f=func['func']
        if marks:
            m=marks[0]
        else:
            m=f.__name__

        Timer.mark(m)

        try:
            ret = f(*args,**kargs)

            if len(marks)>1:
                Timer.end(m, marks[1])
            else:
                Timer.end(m)
            return ret
        except:
            if len(marks)>1:
                Timer.end(m, marks[1], True)
            else:
                Timer.end(m)
            raise

    if callable(ags[0]):
        func['func']=ags[0]
        return timing
    
    marks=ags
    def wrapper(*args):
        func['func']=args[0]
        return timing
    return wrapper


        

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    