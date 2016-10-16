# -*- coding: utf-8 -*-
'''
Created on Nov 4, 2015

@author: sunmoonone
'''
import os
import json
from common import utils
from common.utils import ObjectDict
import logging
import time
import struct
from copy import deepcopy, copy
import atexit
import shutil

class WorkOnNewWithUnfinished(Exception):
    pass

class WorkOnFinished(Exception):
    pass

class GatewayRouteFailed(Exception):
    pass

class Task(ObjectDict):

    def __str__(self):
        return '<Task %s:%s>' % (self.id, self.offset)

def objectdict_json_hook(dct):
    if 'task' in dct:
        dct['task']=Task(dct['task'])
    return ObjectDict(dct)

class Progress(object):
    '''record data of steps of one work flow to file
    
    data is appended to the end of the log file. unchanging variable values are being recorded once to save disk space
    '''
    INT_SIZE=4 
    
    def __init__(self, path, header_names=(), non_header_names=()):
        '''
        not recursively
        '''
        self._path=path
        self._header_names=header_names 
        self._non_header_names=non_header_names 

        self._compacting=False
        self._load()
        atexit.register(self.close)
    
    def _load(self):
        '''
        read all data in file and compact as one dict
        @return: ObjectDcit
        '''
        if not os.path.exists(self._path):
            utils.makedirs(self._path)
            self._fd = open(self._path,'w+b')
        else:
            self._fd = open(self._path,'r+b')

        fd=self._fd

        bak = '%s.bak' % self._path
        if os.path.exists(bak):
            fd = open(bak,'r+b')

        data = ObjectDict()
        while 1:
            bs = fd.read(self.INT_SIZE)
            if bs:
                data_size = struct.unpack('I',bs)[0]
                if data_size:
                    bs = fd.read(data_size)
                    data.update(json.loads(bs.decode('utf8'),object_hook=objectdict_json_hook))
                else:
                    raise Exception('corrupted progress data')
            else:
                break

        if fd!=self._fd:
            fd.close()
            self.compact(True)

        self._data=data
        

    def append(self, data, no_mtime=False):
        '''append data to the end of the log file

        @param data: a dict 
        '''
        data = copy(data)
        for k,v in data.items():
            if k in self._header_names or (self._non_header_names and k not in self._non_header_names): 
                #skip already recorded headers
                if k in self._data and v == self._data[k]:
                    del data[k]
        
        if data:
            
            if not no_mtime:
                t = int( round( time.time()* 1000))
                if t != self.getmtime():
                    data['__mtime__'] = t

            self._data.update(data)

            bs=json.dumps(data).encode('utf8')
            self._fd.write(struct.pack('I',len(bs)))
            self._fd.write(bs)
            self._fd.flush()
    
    def _setmtime(self,v):
        if self.getmtime() == v:
            return
        self._data['__mtime__'] = v
        bs=json.dumps({'__mtime__':v}).encode('utf8')
        self._fd.write(struct.pack('I',len(bs)))
        self._fd.write(bs)
        self._fd.flush()
    
    def get(self,name,default=None):
        if name in self._data:
            return self._data[name]
        return default
    
    def getmtime(self):
        return self._data['__mtime__'] if '__mtime__' in self._data else 0
    
    def compact(self, restore=False):
        '''compact data into to one dict in file
        '''
        if self._compacting:
            return

        if not restore:
            try:
                self._compacting=True
                #1. backup file (rm if it exists)
                bak = '%s.bak' % self._path
                if os.path.exists(bak):
                    os.unlink(bak)
                shutil.copy2(self._path, bak) 
            except:
                self._compacting=False
                raise
        
        #2. compact data
        try:
            bs=json.dumps(self._data).encode('utf8')
        except:
            if not restore:
                os.unlink(bak)
            self._compacting=False
            raise 

        self._fd.truncate(0)
        self._fd.seek(0)
        self._fd.write(struct.pack('I',len(bs)))
        self._fd.write(bs)
        self._fd.flush()
        
        self._compacting=False
            
        #3. rm backup file
        os.unlink(bak)

    def copy_data(self, deep=False):
        if deep:
            return deepcopy(self._data)
        return copy(self._data)
    
    def get_data(self):
        return self._data

    def close(self):
        if self._fd:
            try:
                self._fd.close()
            except:
                pass

class WorkFlow(object):

    def __init__(self,progress_file,beat_func=None,start=None,end=None, sleeper=None, init_progress=None):
        self._context=Context()
        if start:
            self.set_start(start)
        if end:
            self.set_end(end)

        self._beat_func=beat_func
        self._sleeper=sleeper
        self._logger=None
        self._stopped=False
        self._paused=False
        self._to_skip=None
        self._forward_to=None

        self._current_node=None
        
        self._progress = Progress(progress_file, non_header_names=('_not_exist_key_',))
        if init_progress:
            self._progress.append(init_progress)
        self._data = self._progress.copy_data(True)

        if '_global' not in self._data:
            self._data['_global']=ObjectDict()
        if '_internal' not in self._data:
            self._data['_internal']=ObjectDict()
    
    def _set_cur_node(self,node):
        self._current_node=node
        self._data._internal.current_node_id=node.id

    def _get_cur_node(self):
        if self._current_node:
            return self._current_node
        return self.get_node(self._data._internal.current_node_id)   

    current_node=property(fget=_get_cur_node,fset=_set_cur_node)
    
    def _set_logger(self,logger):
        self._logger=logger
    
    def _get_logger(self):
        if not self._logger:
            logger = logging.getLogger('workflow')
            self._logger=logger
        return self._logger

    logger=property(fget=_get_logger,fset=_set_logger)
    
    def put_data(self,name,value):
        self._data[name]=value
    
    def put_global_data(self,name,value):
        self._data['_global'][name]=value

    def get_global_data(self,name,default=None):
        '''
        data that spans across tasks
        '''
        if self._data['_global'] and name in self._data['_global']:
            return self._data['_global'][name]
        return default
    
    def get_data(self,name,default=None):
        '''
        data that spans in one task
        '''
        if name not in self._data:
            return default
        return self._data[name]

    def get_context(self):
        return self._context
    
    def get_current_return(self):
        return self._data._internal.current_return
    
    def _dump_progress(self, call_stat):
        self._data._internal.call_stat=call_stat
        self._progress.append(self._data)
    
    def get_progress_data(self):
        return self._progress.get_data()

    def set_start(self,func):
        if isinstance(func, Node):
            self._start=func
        else:
            self._start = Node(func, self._context)
        return self._start

    def set_end(self,end=None):
        if not end:
            self._end = EndNode(self._context)
        else:
            self._end=end
        return self._end

    def get_node(self,node_id):
        if not node_id:
            return self._start
        return self._context.get(node_id)

    def get_task(self):
        return self._data.task
    
    def has_unfinished(self):
        if self._data.task:
            node=self.get_node(self._data._internal.current_node_id)
            if not isinstance(node, EndNode):
                return True 
        return False
    
    def stop(self):
        self._stopped=True

    def pause(self):
        self._paused=True
        self.logger.debug('workflow paused')

    def resume(self):
        self._paused=False
        self.logger.debug('workflow resumed')
    
    def skip(self, task_id, forcmd='skip'):
        '''skip current task
        '''
        if(not self._data.task):return False
        if(task_id != self._data.task.id): return False

        self._to_skip=task_id
        self._forcmd=forcmd
        return True
    
    def forward_to(self,node_id):
        '''forward to node
        '''
        self._forward_to=node_id
    
    def sleep(self,n):
        if self._sleeper:
            self._sleeper(n)
        else:
            time.sleep(n)
        
    
    def work(self, task, allow_re_work=False):
        '''work on task
        work will be resumed from the last node where stopped
        '''

        node=self.get_node(self._data._internal.current_node_id)

        if isinstance(node, EndNode):
            if not allow_re_work and self._data.task and str(self._data.task) == str(task):
                raise WorkOnFinished('not allowed to work on finished task')

            self._data=ObjectDict(_internal=ObjectDict(),_global=self._data._global)
            self.current_node = node = self._start

        else:
            if self._data.task and str(self._data.task) != str(task):
                if self._to_skip and self._to_skip==self._data.task.id:
                    pass
                else:
                    raise WorkOnNewWithUnfinished('new task: %s is different with task in progress: %s' % (str(task), str(self._data.task)))
            elif self._data.task:
                self.logger.info('work on unfinished task: %s' % self._data.task)
                

        self._data.task=task
        self._stopped=False

        while 1:
            if self._beat_func:
                self._beat_func()

            if self._stopped:
                self.logger.info('stop workflow')
                break

            if self._paused:
                self.sleep(1)
                continue
            
            if self._to_skip and (self._to_skip == self._data.task.id or str(self._to_skip) == str(self._data.task)):
                ret = self.forward_to_end()
                if ret:
                    self._to_skip=None
                    forcmd=self._forcmd
                    self._forcmd=None
                    return forcmd
                else:
                    self._to_skip=None
                    self.logger.error('skip task:%s failed' % self._to_skip)

            self.current_node=node
            
            if self._forward_to:
                node = self.get_node(self._forward_to)
                self._current_node=node
                self._forward_to=None
                self.logger.debug('forward to %s' % node)

            if not self._data._internal.call_stat:
                self._dump_progress(0)
                self._data._internal.current_return = node.func()
                self._dump_progress(1)
            
            if isinstance(node, ConditionNode):
                if self._data._internal.current_return:
                    node = node.yes
                else:
                    node = node.no
            elif isinstance(node, GatewayNode):
                node = node.route(self._data._internal.current_return)
            else:
                node=node.next
            #important!!!
            self._data._internal.call_stat=0
            
            if isinstance(node, EndNode):
                self.current_node=node
                self._dump_progress(0)
                self.logger.debug('work to end node')
                self._progress.compact()
                return True

    def forward_to_end(self):
        if self._data.task:
            self.current_node=self._end
            self._dump_progress(0)
            self.logger.debug('forward to end of task:%s' % self._data.task)
            self._progress.compact()
            return True


class Context(object):

    def __init__(self):
        self._counter=0
        self._nodes={}

    def id(self):
        self._counter+=1
        return self._counter
    
    def put(self, id_, node):
        self._nodes[id_]=node

    def get(self, id_):
        return self._nodes[id_]

class Node(object):
    
    def __init__(self,func, context):
        if not func:
            raise Exception('function required')
        self.func=func
        self.id=context.id()
        self.next=None
        self.context=context
        context.put(self.id, self)

    def set_next(self, func):
        if isinstance(func, Node):
            self.next=func
        else:
            self.next = Node(func, self.context)
        return self.next

    def set_next_condition(self,func):
        if isinstance(func, Node):
            self.next=func
        else:
            self.next = ConditionNode(func, self.context)
        return self.next
    
    def set_next_gateway(self,func):
        if isinstance(func, GatewayNode):
            self.next=func
        else:
            self.next = GatewayNode(func, self.context)
        return self.next
    
    def __str__(self):
        return '<%s:%s,%s>' % (self.__class__.__name__,self.id,self.func.__name__ if self.func else None)

    def __repr__(self):
        return '<%s:%s,%s>' % (self.__class__.__name__,self.id,self.func.__name__ if self.func else None)

class EndNode(Node):
    def __init__(self,context):
        self.func=None
        self.id=context.id()
        self.context=context
        context.put(self.id, self)


class ConditionNode(Node):
    def __init__(self, func, context):
        Node.__init__(self, func, context)
        self.yes=None
        self.no=None
    
    def set_yes(self,func):
        if isinstance(func, Node):
            self.yes=func
        else:
            self.yes = Node(func, self.context)
        return self.yes

    def set_no(self,func):
        if isinstance(func, Node):
            self.no=func
        else:
            self.no=Node(func,self.context)
        return self.no

class GatewayNode(Node):
    def __init__(self, func, context):
        Node.__init__(self, func, context)
        self._route={}
    
    def set_on(self, val, func):
        if not isinstance(func, Node):
            func = Node(func,self.context)
        self._route[val]=func
        return func

    def route(self,val):
        if val in self._route:
            return self._route[val]
        raise GatewayRouteFailed('no target node for val: %s' % val)
    

class MuliOutConditionNode(Node):
    def __init__(self,func, context):
        raise NotImplementedError('not implemented')
