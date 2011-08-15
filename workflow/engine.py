
#######################################################################################
## Copyright (c) 2010-2011 CERN                                                      ##
## All rights reserved.                                                              ##
##                                                                                   ##
## Redistribution and use in source and binary forms, with or without modification,  ##
## are permitted provided that the following conditions are met:                     ##
##                                                                                   ##
##     * Redistributions of source code must retain the above copyright notice,      ##
##       this list of conditions and the following disclaimer.                       ##
##     * Redistributions in binary form must reproduce the above copyright notice,   ##
##       this list of conditions and the following disclaimer in the documentation   ##
##       and/or other materials provided with the distribution.                      ##
##     * Neither the name of the author nor the names of its contributors may be     ##
##       used to endorse or promote products derived from this software without      ##
##       specific prior written permission.                                          ##
##                                                                                   ##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND   ##
## ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED     ##
## WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.##
## IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,  ##
## INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    ##
## BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,     ##
## DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF   ##
## LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE   ##
## OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED ##
## OF THE POSSIBILITY OF SUCH DAMAGE.                                                ##
##                                                                                   ##
#######################################################################################

import logging # we are not using the newseman logging to make this library independent
import new
import copy
import pickle
import sys

DEBUG = False
LOGGING_LEVEL = logging.INFO
LOG = None

class WorkflowTransition(Exception): pass # base class for WFE
class WorkflowError(Exception): pass # general error

class StopProcessing(WorkflowTransition): pass #stops current workflow
class HaltProcessing(WorkflowTransition): pass #halts the workflow (can be used for nested wf engines)
class ContinueNextToken(WorkflowTransition): pass # can be called many levels deep, jumps up to next token
class JumpTokenForward(WorkflowTransition): pass
class JumpTokenBack(WorkflowTransition): pass

class JumpCallForward(WorkflowTransition): pass #in one loop [call, call...] jumps x steps forward
class JumpCallBack(WorkflowTransition): pass #in one loop [call, call...] jumps x steps forward
class BreakFromThisLoop(WorkflowTransition): pass #break from this loop, but do not stop processing

class WorkflowMissingKey(WorkflowError): pass # when trying to use unregistered workflow key


class GenericWorkflowEngine(object):
    """Wofklow engine is a Finite State Machine with memory
    It is used to execute set of methods in a specified order.

    example:

        from merkur.workflows.parts import load_annie, load_seman
        from newseman.general.workflow import patterns as p

        workflow = [
                load_seman_components.workflow,
                p.IF(p.OBJ_GET(['path', 'text'], cond='any'),
                     [ p.TRY(g.get_annotations(), retry=1,
                                onfailure=p.ERROR('Error in the annotation workflow'),
                                verbose=True),
                       p.IF(p.OBJ_GET('xml'),
                             translate_document.workflow)
                    ])
                ]

        This workflow is then used as:
            wfe = GenericWorkflowEngine()
            wfe.setWorkflow(workflow)
            wfe.process([{foo:bar}, {foo:...}])

    This workflow engine instance can be freezed and restarted, it remembers
    its internal state and will pick up processing after the last finished
    task.

        import pickle
        s = pickle.dumps(wfe)

    However, when restarting the workflow, you must initialize the workflow
    tasks manually using their original definition

        wfe = pickle.loads(s)
        wfe.setWorkflow(workflow)

    It is also not possible to serialize WFE when custom factory
    tasks were provided. If you attempt to serialize such a WFE instance,
    it will raise exception. If you want to serialize
    WFE including its factory hooks and workflow callbacks, use the
    PhoenixWorkflowEngine class instead.


    """

    def __init__(self,
                 processing_factory=None,
                 callback_chooser=None,
                 before_processing=None,
                 after_processing=None):

        self._picklable_safe = True
        for name, x in [('processing_factory', processing_factory),
                        ('callback_chooser', callback_chooser),
                        ('before_processing', before_processing),
                        ('after_processing', after_processing)]:
            if x:
                if not callable(x):
                    raise WorkflowError('Callback "%s" must be a callable object' % name)
                else:
                    setattr(self, name, x)
                    self._picklable_safe = False

        self._callbacks = {}
        self._store = {}
        self._objects = [] # tmp storage of processed objects
        self._i = [-1, [0]] # holds ids of the currently processed object and task
        self._unpickled = False
        self.log = logging.getLogger("workflow.%s" % self.__class__) # default logging

    def __getstate__(self):
        if not self._picklable_safe:
            raise pickle.PickleError("The instance of the workflow engine cannot be serialized, "
            "because it was constructed with custom, user-supplied callbacks. Either use"
            "PickableWorkflowEngine or provide your own __getstate__ method.")
        return {'_store':self._store, '_objects': self._objects,
                '_i': self._i, '_callbacks': {}, 'log': self.log}


    def __setstate__(self, state):
        self._store = state['_store']
        self._objects = state['_objects']
        self._i = state['_i']
        self._callbacks = state['_callbacks']
        self.log = state['log']
        if len(self._objects) < self._i[0]:
            raise pickle.PickleError("The workflow instance inconsistent state, too few objects")
        self._unpickled = True


    def setLogger(self, logger):
        """The logger instance must be pickable if the serialization should work"""
        self.log = logger


    def continueNextToken(self):
        """Continue with the next token"""
        raise ContinueNextToken

    def stopProcessing(self):
        """Break out, stops everything (in the current wfe)"""
        raise StopProcessing

    def haltProcessing(self):
        """Halt the workflow (stop also any parent wfe)"""
        raise HaltProcessing

    def jumpTokenForward(self, offset):
        """Jumps to xth token"""
        raise JumpTokenForward(offset)

    def jumpTokenBack(self, offset):
        """Returns x tokens back - be careful with circular loops"""
        raise JumpTokenBack(offset)

    def jumpCallForward(self, offset):
        """Jumps to xth call in this loop"""
        raise JumpCallForward(offset)

    def jumpCallBack(self, offset):
        """Returns x calls back in the current loop - be careful with circular loop"""
        raise JumpCallBack(offset)

    def breakFromThisLoop(self):
        """Stops in the current loop but continues in those above"""
        raise BreakFromThisLoop

    def configure(self, **kwargs):
        """Method to set attributes of the workflow engine - use with extreme care
        (well, you can set up the attrs directly, I am not protecting them, but
        that is not nice)
        Used mainly if you want to change the engine's callbacks - if processing factory
        before_processing, after_processing

        @var **kwargs: dictionary of values
        """
        for (key, value) in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise WorkflowError("Object %s does not have attr %s - it is not allowed to set nonexisting attribute (and you don't circumvent interface, do you?)" % (str(self), key))

    def process(self, objects):
        """Start processing
        @param  objects: either a list of object or
                instance of TokenizedDocument
        @return: You never know what will be returned from the workflow ;-)
                But many exceptions can be raised, so watch out for them,
                if there happened an exception, you can be sure something
                wrong happened (something that your workflow should handle
                and didn't). Workflow engine is not interfering into the
                processing chain, it is not catching exceptions for you.
        """
        if isinstance(objects, list):
            if not len(objects):
                self.log.warning('List of objects is empty. Running workflow on empty set has no effect.')
            return self.processing_factory(objects, self)
        elif hasattr(objects, 'TokenizedDocument') and objects.TokenizedDocument:
            if not len(objects.tokens()):
                self.log.warning('Running workflow on empty TokenizedDocument set has no effect.')
            return self.processing_factory(objects.tokens(), self)
        else:
            raise WorkflowError('Passed in object %s is neither list nor TokenizedDocument' % (objects.__class__))



    @staticmethod
    def before_processing(objects, self):
        """Standard pre-processing callback - saves a pointer to the processed objects"""
        #self.reset()
        self._objects = objects


    @staticmethod
    def after_processing(objects, self):
        """Standard post-processing callback, basic cleaning"""
        self._objects = []
        self._i = [-1, [0]]

    @staticmethod
    def callback_chooser(obj, self):
        """There are possibly many workflows inside this workflow engine
        and they are meant for different types of objects, this method
        should choose and return the callbacks appropriate for the currently
        processed object
        @var obj: currently processed object
        @var eng: the workflow engine object
        @return: set of callbacks to run
        """
        if hasattr(obj, 'getFeature'):
            t = obj.getFeature('type')
            if t:
                return self.getCallbacks(t)
        else:
            return self.getCallbacks('*') #for the non-token types return default workflows

    @staticmethod
    def processing_factory(objects, self):
        """Default processing factory, will process objects in order

        @var objects: list of objects (passed in by self.process())
        @keyword cls: engine object itself, because this method may
            be implemented by the standalone function, we pass the
            self also as a cls argument

        As the WFE proceeds, it increments the internal counter, the
        first position is the number of the element. This pointer increases
        before the object is taken

        2nd pos is reserved for the array that points to the task position.
        The number there points to the task that is currently executed;
        when error happens, it will be there unchanged. The pointer is
        updated after the task finished running.
        """

        self.before_processing(objects, self)

        i = self._i

        while i[0] < len(objects)-1 and i[0] >= -1: # negative index not allowed, -1 is special
            i[0] += 1
            obj = objects[i[0]]
            callbacks = self.callback_chooser(obj, self)
            if callbacks:
                try:
                    self.run_callbacks(callbacks, objects, obj)
                    i[1] = [0] #reset the callbacks pointer
                except StopProcessing:
                    if DEBUG:
                        self.log.debug("Processing was stopped: '%s' (object: %s)" % (str(callbacks), repr(obj)))
                    break
                except JumpTokenBack, step:
                    if step.args[0] > 0:
                        raise WorkflowError("JumpTokenBack cannot be positive number")
                    if DEBUG:
                        self.log.debug('Warning, we go back [%s] objects' % step.args[0])
                    i[0] = max(-1, i[0] - 1 + step.args[0])
                    i[1] = [0] #reset the callbacks pointer
                except JumpTokenForward, step:
                    if step.args[0] < 0:
                        raise WorkflowError("JumpTokenForward cannot be negative number")
                    if DEBUG:
                        self.log.debug('We skip [%s] objects' % step.args[0])
                    i[0] = min(len(objects), i[0] - 1 + step.args[0])
                    i[1] = [0] #reset the callbacks pointer
                except ContinueNextToken:
                    if DEBUG:
                        self.log.debug('Stop processing for this object, continue with next')
                    i[1] = [0] #reset the callbacks pointer
                    continue
                except HaltProcessing:
                    if DEBUG:
                        self.log.debug('Processing was halted at step: %s' % i)
                        # reraise the exception, this is the only case when a WFE can be completely
                        # stopped
                    raise

        self.after_processing(objects, self)



    def run_callbacks(self, callbacks, objects, obj, indent=0):
        """This method will execute callbacks in the workflow
        @var callbacks: list of callables (may be deep nested)
        @var objects: list of processed objects
        @var obj: currently processed object
        @keyword indent: int, indendation level - the counter
            at the indent level is increases after the task has
            finished processing; on error it will point to the
            last executed task position.
                 The position adjusting also happens after the
            task has finished.
        """
        c = 0 #Just a counter for debugging
        y = self._i[1]  #Position of the task
        while y[indent] < len(callbacks):
            # jump to the appropriate place if we were restarted
            if len(y)-1 > indent:
                self.log.debug('Fast-forwarding to the position:callback = %s:%s' % (indent, y[indent]))
                #print 'indent=%s, y=%s, y=%s, \nbefore=%s\nafter=%s' % (indent, y, y[indent], callbacks, callbacks[y[indent]])
                self.run_callbacks(callbacks[y[indent]], objects, obj, indent+1)
                y.pop(-1)
                y[indent] += 1
                continue
            f = callbacks[y[indent]]
            try:
                c += 1
                if isinstance(f, list) or isinstance(f, tuple):
                    y.append(0)
                    self.run_callbacks(f, objects, obj, indent+1)
                    y.pop(-1)
                    y[indent] += 1
                    continue
                if DEBUG:
                    self.log.debug("Running (%s%s.) callback '%s' for obj: %s" % (indent * '-', c, f.__name__, repr(obj)))
                self.execute_callback(f, obj)
                if DEBUG:
                    self.log.debug('+ok')
            except BreakFromThisLoop:
                if DEBUG:
                    self.log.debug('Break from this loop')
                return
            except JumpCallBack, step:
                if DEBUG:
                    self.log.debug('Warning, we go [%s] calls back' % step.args[0])
                if step.args[0] > 0:
                    raise WorkflowError("JumpCallBack cannot be positive number")
                y[indent] = max(-1, y[indent] + step.args[0]-1)
            except JumpCallForward, step:
                if DEBUG:
                    self.log.debug('We skip [%s] calls' % step.args[0])
                if step.args[0] < 0:
                    raise WorkflowError("JumpCallForward cannot be negative number")
                y[indent] = min(len(callbacks), y[indent] + step.args[0]-1)
            y[indent] += 1
        #y[indent] -= 1 # adjust the counter so that it always points to the last executed task

    def setPosition(self, obj_pos, task_pos):
        """Sets the internal pointers (of current state/obj)
        @var obj_pos: (int) index of the currently processed object
            After invocation, the engine will grab the next obj
            from the list
        @var task_pos: (list) multidimensional one-element list
            that says at which level the task should restart. Example:
            6th branch, 2nd task = [5, 1]
        """
        #TODO: check that positions are not out-of-bounds
        self._i[0] = obj_pos
        self._i[1] = task_pos

    def execute_callback(self, callback, obj):
        """Executes the callback - override this method to implement logging"""

        callback(obj, self)
        #print self._i


    def getCallbacks(self, key='*'):
        """Returns callbacks for the given workflow
        @keyword key: name of the workflow (default: *)
                if you want to get all configured workflows
                pass None object as a key
        @return: list of callbacks
        """
        if key:
            try:
                return self._callbacks[key]
            except KeyError, e:
                raise WorkflowMissingKey('No workflow is registered for the key: %s. Perhaps you forgot to load workflows or the workflow definition for the given key was empty?' % key)
        else:
            return self._callbacks

    def addCallback(self, key, func, before=None, after=None, relative_weight=None):
        '''Inserts one callable to the stack of the callables'''
        try:
            if func: #can be None
                self.getCallbacks(key).append(func)
        except WorkflowMissingKey:
                self._callbacks[key] = []
                return self._callbacks[key].append(func)
        except Exception, e:
            self.log.debug('Impossible to add callback %s for key: %s' % (str(func), key))
            self.log.debug(e)

    def addManyCallbacks(self, key, list_or_tuple):
        list_or_tuple = list(self._cleanUpCallables(list_or_tuple))
        for f in list_or_tuple:
            self.addCallback(key, f)

    @classmethod
    def _cleanUpCallables(cls, callbacks):
        """helper method to remove non-callables from the passed-in callbacks"""
        if callable(callbacks):
            yield callbacks
        for x in callbacks:
            if isinstance(x, list):
                yield list(cls._cleanUpCallables(x))
            elif isinstance(x, tuple):
                # tumples are simply converted to normal members
                for fc in cls._cleanUpCallables(x):
                    yield fc
            elif x is not None:
                yield x

    def removeAllCallbacks(self):
        """Removes all the tasks from the workflow engine instance"""
        self._callbacks = {}

    def removeCallbacks(self, key):
        """for the given key, remove callbacks"""
        try:
            del(self._callbacks[key])
        except KeyError:
            pass

    def reset(self):
        """Empties the stack memory"""
        self._i = [-1, [0]]
        self._store = {}

    def replaceCallbacks(self, key, funcs):
        """replace processing workflow with a new workflow"""
        list_or_tuple = list(self._cleanUpCallables(funcs))
        self.removeCallbacks(key)
        for f in list_or_tuple:
            self.addCallback(key, f)

    def setWorkflow(self, list_or_tuple):
        """Sets the (default) workflow which will be run when
        you call process()
        @var list_or_tuple: workflow configuration
        """
        if not isinstance(list_or_tuple, list) or not isinstance(list_or_tuple, tuple):
            list_or_tuple = (list_or_tuple,)
        self.replaceCallbacks('*', list_or_tuple)

    def setVar(self, key, what):
        """Stores the obj in the internal stack"""
        self._store[key] = what

    def getVar(self, key, default=None):
        """returns named obj from internal stack. If not found, returns None.
        @param key: name of the object to return
        @keyword default: if not found, what to return instead (if this arg
            is present, the stack will be initialized with the same value)
        @return: anything or None"""
        try:
            return self._store[key]
        except:
            if default is not None:
                self.setVar(key, default)
            return default

    def hasVar(self, key):
        """Returns True if parameter of this name is stored"""
        if key in self._store:
            return True

    def delVar(self, key):
        """Deletes parameter from the internal storage"""
        if key in self._store:
            del self._store[key]

    def getCurrObjId(self):
        """Returns id of the currently processed object"""
        return self._i[0]

    def getCurrTaskId(self):
        """Returns id of the currently processed task. Note that the return value of this method is not thread-safe."""
        return self._i[1]

    def getObjects(self):
        """Returns iterator for walking through the objects"""
        i = 0
        for obj in self._objects:
            yield (i, obj)
            i += 1

    def restart(self, obj, task, objects=None):
        """Restart the workflow engine after it was deserialized

        """

        if self._unpickled is not True:
            raise Exception("You can call this method only after loading serialized engine")
        if len(self.getCallbacks(key=None)) == 0:
            raise Exception("The callbacks are empty, did you set workflows?")

        # set the point from which to start processing
        if obj == 'prev': # start with the previous object
            self._i[0] -= 2 #TODO: check if there is any object there
        elif obj == 'current': # continue with the current object
            self._i[0] -= 1
        elif obj == 'next':
            pass
        else:
            raise Exception('Unknown start point for object: %s' % obj)

        # set the task that will be executed first
        if task == 'prev': # the previous
            self._i[1][-1] -= 1
        elif obj == 'current': # restart the task again
            self._i[1][-1] -= 0
        elif obj == 'next': # continue with the next task
            self._i[1][-1] += 1
        else:
            raise Exception('Unknown start pointfor task: %s' % obj)

        if objects:
            self.process(objects)
        else:
            self.process(self._objects)

        self._unpickled = False


class PhoenixWorkflowEngine(GenericWorkflowEngine):
    """Implementation of the GenericWorkflowEngine which is able to be
    *serialized* and re-executed also with its workflow tasks - without knowing
    their original definition. This implementation depends on the
    picloud module - http://www.picloud.com/. The module must be
    installed in the standard location.

    """

    def __init__(self, *args, **kwargs):
        super(PhoenixWorkflowEngine, self).__init__(*args, **kwargs)
        from cloud import serialization
        self._picloud_serializer = serialization


    def __getstate__(self):
        out = super(PhoenixWorkflowEngine, self).__getstate__()
        cbs = self.getCallbacks(key=None)
        out['_callbacks'] = self._picloud_serializer.serialize(cbs, needsPyCloudSerializer=True)
        factory_calls = {}
        for name in ('processing_factory', 'callback_chooser', 'before_processing', 'after_processing'):
            c = getattr(self, name)
            if c.__class__ != 'PhoenixWorkflowEngine':
                factory_calls[name] = c
        out['factory_calls'] = self._picloud_serializer.serialize(factory_calls, needsPyCloudSerializer=True)
        return out

    def __setstate__(self, state):
        from cloud import serialization
        self._picloud_serializer = serialization

        state['_callbacks'] = self._picloud_serializer.deserialize(state['_callbacks'])
        super(PhoenixWorkflowEngine, self).__setstate__(state)
        factory_calls = self._picloud_serializer.deserialize(state['factory_calls'])
        for k,v in factory_calls.items():
            setattr(self, k, v)







# ------------------------------------------------------------- #
#                       helper methods/classes                  #
# ------------------------------------------------------------- #

def duplicate_engine_instance(eng):
    """creates a new instance of the workflow engine based on existing instance"""
    #new_eng = copy.deepcopy(eng)
    #new_eng.removeAllCallbacks()
    #new_eng.reset()


    new_eng = eng.__class__(processing_factory=eng.processing_factory,
                      callback_chooser=eng.callback_chooser,
                      before_processing=eng.before_processing,
                      after_processing=eng.after_processing)
    return new_eng


def get_logger(name):
    """Creates a logger for you - with the parent logger and
    common configuration"""
    if name[0:8] != 'workflow' and len(name) > 8:
        sys.stderr.write("Warning: you are creating a logger without 'workflow' as a root (%s),"
        "this means that it will not share workflow settings and cannot be administered from one place" % name)
    if LOG:
        logger = LOG.manager.getLogger(name)
    else:
        logger = logging.getLogger(name)
        hdlr = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter('%(levelname)s %(asctime)s %(name)s:%(lineno)d    %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(LOGGING_LEVEL)
        logger.propagate = 0
    if logger not in _loggers:
        _loggers.append(logger)
    return logger

def reset_all_loggers(level):
    """Set logging level for every active logger - beware, if the global
    manager level is higher, then still nothing will be see. Manager
    level has precedence - use set_global_level
    """
    for l in _loggers:
        l.setLevel(LOGGING_LEVEL)

def set_global_level(level):
    """Sets the global level to the manager, the parent manager of all
    the newseman loggers. With this one call, you can switch off all
    loggers at once. But you can't enable them using this call, because
    every logger may have a specific log level
    """
    global LOGGING_LEVEL
    LOGGING_LEVEL = int(level)
    LOG.manager.disable = LOGGING_LEVEL - 1


_loggers = []
LOG = get_logger('workflow')
set_global_level(LOGGING_LEVEL)





