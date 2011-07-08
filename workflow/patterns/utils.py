
import inspect
import traceback
import sys

# TODO - deal with this import, to be newseman agnostic
from newseman.general.workflow.engine import WorkflowTransition, duplicate_engine_instance


def RUN_WF(workflow, engine=None,
           processing_factory = None,
           callback_chooser = None,
           before_processing = None,
           after_processing = None,
           data_connector = None,
           pass_eng = [],
           pass_always = None,
           outkey = 'RUN_WF',
           reinit=False):
    """Task for running other workflow - ie. new workflow engine will
    be created and the workflow run. The workflow engine is garbage
    collected together with the function. Therefore you can run the
    function many times and it will reuse the already-loaded WE. In fact
    this WE has an empty before_processing callback.

    @see before_processing callback for more information.

    @var workflow: normal workflow tasks definition
    @keyword engine: class of the engine to create WE, if None, the new
        WFE instance will be of the same class as the calling WFE. 
        Attention, changes in the classes of the WFE instances may have
        many consequences, so be careful. For example, if you use 
        serialiazable WFE instance, but you create another instance of WFE
        which is not serializable, then you will be in problems.
    @keyword processing_factory: WE callback
    @keyword callback_chooser: WE callback
    @keyword before_processing: WE callback
    @keyword after_processing: WE callback
    ---
    @keyword data_connector: callback which will prepare data and pass
        the corrent objects into the workflow engine (from the calling
        engine into the called WE), if not present, the current obj is
        passed (possibly wrapped into a list)
    @keyword pass_eng: list of keys corresponding to the values, that should
        be passed from the calling engine to the called engine. This is
        called only once, during initialization.
    @keyword outkey: if outkey is present, the initialized new
        workflow engine is stored into the calling workflow engine
        so that you can get access to it from outside. This instance
        will be available at runtime
    @keyword reinit: if True, wfe will be re-instantiated always
        for every invocation of the function
    """

    store = []

    def x(obj, eng=None):
        
        # decorate the basic callback to make sure the objects of the calling
        # engine are always there. But since the default callback no longer
        # calls reset(), this is no longer necessary
        #if not before_processing: 
        #    old = eng.before_processing
        #    def _before_processing(obj, eng):
        #        old(obj, eng)
        #        setattr(eng, '_objects', obj)
        #else:
        #    _before_processing = None
        
        if engine: #user supplied class
            engine_cls = engine
        else:
            engine_cls = eng.__class__
        
        # a lot of typing, but let's make it explicit what happens...
        _processing_factory = processing_factory or engine_cls.processing_factory
        _callback_chooser = callback_chooser or engine_cls.callback_chooser
        _before_processing = before_processing or engine_cls.before_processing
        _after_processing = after_processing or engine_cls.after_processing
        
        if not store:
            store.append(engine_cls(processing_factory=_processing_factory, 
                                    callback_chooser=_callback_chooser, 
                                    before_processing=_before_processing, 
                                    after_processing=_after_processing))
            store[0].setWorkflow(workflow)

        if reinit: # re-init wfe to have a clean plate
            store[0] = engine_cls(processing_factory=_processing_factory, 
                                    callback_chooser=_callback_chooser, 
                                    before_processing=_before_processing, 
                                    after_processing=_after_processing)
            store[0].setWorkflow(workflow)

        wfe = store[0]

        if outkey:
            eng.setVar(outkey, wfe)

        # pass data from the old wf engine to the new one
        to_remove = []
        for k in pass_eng:
            wfe.setVar(k, eng.getVar(k))
            if not pass_always and not reinit:
                to_remove.append(k)
        if to_remove:
            for k in to_remove:
                pass_eng.remove(k)


        if data_connector:
            data = data_connector(obj, eng)
            wfe.process(data)
        else:
            if not isinstance(obj, list):
                wfe.process([obj])
            else:
                wfe.process(obj)
    x.__name__ = 'RUN_WF'
    return x

# -------------------------- useful structures------------------------------------- #

def EMPTY_CALL(obj, eng):
    """Empty call that does nothing"""
    pass

def ENG_GET(something):
    """this is the same as lambda obj, eng: eng.getVar('something')
    @var something: str, key of the object to retrieve
    @return: value of the key from eng object
    """
    def x(obj, eng):
        return eng.getVar(something)
    x.__name__ = 'ENG_GET'
    return x

def ENG_SET(key, value):
    """this is the same as lambda obj, eng: eng.setVar('key', value)
    @var key: str, key of the object to retrieve
    @var value: anything
    @attention: this call is executed when the workflow is created
        therefore, the key and value must exist at the time
        (obj and eng don't exist yet)
    """
    def _eng_set(obj, eng):
        return eng.setVar(key, value)
    _eng_set.__name__ = 'ENG_SET'
    return _eng_set

def OBJ_GET(something, cond='all'):
    """this is the same as lambda obj, eng: something in obj and obj[something]
    @var something: str, key of the object to retrieve or list of strings
    @keyword cond: how to evaluate several keys, all|any|many
    @return: value of the key from obj object, if you are looking at several
        keys, then a list is returned. Watch for empty and None returns!

    """
    def x(obj, eng):
        if isinstance(something, basestring):
            return something in obj and obj[something]
        else:
            if cond.lower() == 'any':
                for o in something:
                    if o in obj and obj[o]:
                        return obj[o]
            elif cond.lower() == 'many':
                r = {}
                for o in something:
                    if o in obj and obj[o]:
                        r[o] = obj[o]
                return r
            else:
                r = {}
                for o in something:
                    if o in obj and obj[o]:
                        r[o] = obj[o]
                    else:
                        return False
                return r


    x.__name__ = 'OBJ_GET'
    return x

def OBJ_SET(key, value):
    """this is the same as lambda obj, eng: obj.__setitem__(key, value)
    @var key: str, key of the object to retrieve
    @var value: anything
    @attention: this call is executed when the workflow is created
        therefore, the key and value must exist at the time
        (obj and eng don't exist yet)
    """
    def x(obj, eng):
        obj[key] = value
    x.__name__ = 'OBJ_SET'
    return x

# ----------------------- error handlling -------------------------------

def ERROR(msg='Error in the workflow'):
    """Throws uncatchable error stopping execution and printing the message"""
    caller = inspect.getmodule(inspect.currentframe().f_back)
    if caller :
        caller = caller.__file__
    else:
        caller = ''
    def x(obj, eng):
        raise Exception('in %s : %s' % (caller, msg))
    x.__name__ = 'ERROR'
    return x

def TRY(onecall, retry=1, onfailure=Exception, verbose=True):
    """Wrap the call in try...except statement and eventually
    retries when failure happens
    @keyword attempts: how many times to retry
    @keyword onfailure: exception to raise or callable to call on failure,
        if callable, then it will receive standard obj, eng arguments
    """

    if not callable(onecall):
        raise Exception('You can wrap only one callable with TRY')

    def x(obj, eng):
        tries = 1 + retry
        i = 0
        while i < tries:
            try:
                onecall(obj, eng)
                break # success
            except WorkflowTransition, msg:
                raise # just let it propagate
            except:
                if verbose:
                    eng.log.error('Error reported from the call')
                    traceback.print_exc()
                i += 1
                if i >= tries:
                    if isinstance(onfailure, Exception):
                        raise onfailure
                    elif callable(onfailure):
                        onfailure(obj, eng)
                    else:
                        raise Exception('Error after attempting to run: %s' % onecall)

    x.__name__ = 'TRY'
    return x
