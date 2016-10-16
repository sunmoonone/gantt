# -*- coding: utf-8 -*-
'''
Created on Nov 24, 2015

@author: sunmoonone
'''
import time
import math

class Waiter(object):
    '''根据同一方法的连续请求频率，以指数比例增大等待时间，或者以取模运算周期性的增加等待时间
    '''

    def __init__(self, sleeper=None, wait_unit=5, breaker=None):
        '''
        @param sleeper: a callable that will be called to sleep
        @param breaker: a callable that will be called when waiting. Let breaker returns True to quit waiting
        @param wait_unit: time in seconds, the smallest wait unit
        '''
        self._sleeper=sleeper
        self._breaker=breaker
        self._wait_unit=wait_unit
        self._waiting=False
        self._wait_start_time=0
        self._last_wait_caller=None
        self._wait_call_frequency=0
        self._wait_for=None
        self._stop=False
    
    def stat(self):
        if self._waiting:
            return dict(status='waiting',wait_for=self._wait_for,wait_start_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self._wait_start_time)))
        return dict(wait_start_time=0)
    
    def sleep(self,n):
        if self._sleeper:
            self._sleeper(n)
        else:
            time.sleep(n)

    def stop(self):
        self._waiting=False
        self._stop=True
    
    def resume(self):
        self._stop=False

    def set_waiting(self,flag):
        self._waiting=flag
        if flag:
            self._wait_start_time=time.time()
        else:
            self._wait_start_time=0
        
    def wait(self,*args):
        '''
        @param func_name: name of function that invokes wait  
        @param time: optional 

        wait('my_func')
        wait('my_func', 5)
        wait(0)
        '''
        unit=self._wait_unit
        wait_time=unit
        #clear frequency counter
        if args and args[0]==0:
            self._last_wait_caller=None
            self._wait_call_frequency=0
            return
            
        if args and self._last_wait_caller and self._last_wait_caller==args[0]:
            self._wait_for=args[0]
            self._wait_call_frequency+=1

            if len(args)>1:
                if args[1]<1:
                    wait_time = unit* args[1]
                else:
                    wait_time= unit *(1 + self._wait_call_frequency % int(args[1]))
            else:
                wait_time= unit *(1 + math.exp(self._wait_call_frequency))

        elif args and args[0]:
            self._last_wait_caller=args[0]
            self._wait_call_frequency=0
            self._wait_for=args[0]
        else:
            self._wait_for=None
            self._last_wait_caller=None
            self._wait_call_frequency=0

        wait_time=round(wait_time,2)
        self.set_waiting(True)

        while wait_time>0:
            if self._breaker():
                break
            if self._stop:
                break
            if not self._waiting:
                break
            self.sleep(unit)
            wait_time -= unit

        #end while

        self.set_waiting(False)
        self._wait_for=None