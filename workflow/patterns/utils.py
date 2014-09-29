# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

from __future__ import print_function

import inspect
import traceback
import six
import sys
import pstats
import timeit
from workflow.engine import duplicate_engine_instance, WorkflowTransition
try:
    import cProfile
except ImportError:
    import profile as cProfile


def RUN_WF(workflow, engine=None,
           processing_factory=None,
           callback_chooser=None,
           before_processing=None,
           after_processing=None,
           data_connector=None,
           pass_eng=[],
           pass_always=None,
           outkey='RUN_WF',
           reinit=False):
    """Task for running other workflow - i.e. new workflow engine will
    be created and the workflow run. The workflow engine is garbage
    collected together with the function. Therefore you can run the
    function many times and it will reuse the already-loaded WE. In fact
    this WE has an empty before_processing callback.

    @see before_processing callback for more information.

    :param workflow: normal workflow tasks definition
    :param engine: class of the engine to create WE, if None, the new
        WFE instance will be of the same class as the calling WFE.
        Attention, changes in the classes of the WFE instances may have
        many consequences, so be careful. For example, if you use
        serialiazable WFE instance, but you create another instance of WFE
        which is not serializable, then you will be in problems.
    :param processing_factory: WE callback
    :param callback_chooser: WE callback
    :param before_processing: WE callback
    :param after_processing: WE callback
    ---
    :param data_connector: callback which will prepare data and pass
        the corrent objects into the workflow engine (from the calling
        engine into the called WE), if not present, the current obj is
        passed (possibly wrapped into a list)
    :param pass_eng: list of keys corresponding to the values, that should
        be passed from the calling engine to the called engine. This is
        called only once, during initialization.
    :param outkey: if outkey is present, the initialized new
        workflow engine is stored into the calling workflow engine
        so that you can get access to it from outside. This instance
        will be available at runtime
    :param reinit: if True, wfe will be re-instantiated always
        for every invocation of the function
    """
    store = []

    def x(obj, eng=None):

        # decorate the basic callback to make sure the objects of the calling
        # engine are always there. But since the default callback no longer
        # calls reset(), this is no longer necessary
        # if not before_processing:
        #    old = eng.before_processing
        #    def _before_processing(obj, eng):
        #        old(obj, eng)
        #        setattr(eng, '_objects', obj)
        # else:
        #    _before_processing = None

        if engine:  # user supplied class
            engine_cls = engine
        else:
            engine_cls = eng.__class__

        # a lot of typing, but let's make it explicit what happens...
        _processing_factory = processing_factory or \
            engine_cls.processing_factory
        _callback_chooser = callback_chooser or engine_cls.callback_chooser
        _before_processing = before_processing or engine_cls.before_processing
        _after_processing = after_processing or engine_cls.after_processing

        if not store:
            store.append(engine_cls(processing_factory=_processing_factory,
                                    callback_chooser=_callback_chooser,
                                    before_processing=_before_processing,
                                    after_processing=_after_processing))
            store[0].setWorkflow(workflow)

        if reinit:  # re-init wfe to have a clean plate
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

# ------------------------- useful structures ------------------------------- #


def EMPTY_CALL(obj, eng):
    """Empty call that does nothing"""
    pass


def ENG_GET(something):
    """this is the same as lambda obj, eng: eng.getVar('something')
    :param something: str, key of the object to retrieve
    :return: value of the key from eng object
    """
    def x(obj, eng):
        return eng.getVar(something)
    x.__name__ = 'ENG_GET'
    return x


def ENG_SET(key, value):
    """this is the same as lambda obj, eng: eng.setVar('key', value)
    :param key: str, key of the object to retrieve
    :param value: anything
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
    :param something: str, key of the object to retrieve or list of strings
    :param cond: how to evaluate several keys, all|any|many
    :return: value of the key from obj object, if you are looking at several
        keys, then a list is returned. Watch for empty and None returns!

    """
    def x(obj, eng):
        if isinstance(something, six.string_types):
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
    :param key: str, key of the object to retrieve
    :param value: anything
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
    if caller:
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
    :param attempts: how many times to retry
    :param onfailure: exception to raise or callable to call on failure,
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
                break  # success
            except WorkflowTransition:
                raise  # just let it propagate
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
                        raise Exception(
                            'Error after attempting to run: %s' % onecall)

    x.__name__ = 'TRY'
    return x


def PROFILE(call, output=None,
            stats=['time', 'calls', 'cumulative', 'pcalls']):
    """Run the call(s) inside profiler
    :param call: either function or list of functions
        - if it is a single callable, it will be executed
        - if it is a list of callables, a new workflow engine (a duplicate)
          will be created, the workflow will be set with the calls, and
          calls executed; thus by providing list of functions, you are
          actually profiling also the workflow engine!
    :param output: where to save the stats, if empty, it will be printed
          to stdout
    :param stats: list of statistical outputs,
          default is: time, calls, cumulative, pcalls
          @see pstats module for explanation
    """

    def x(obj, eng):
        if isinstance(call, list) or isinstance(call, tuple):
            new_eng = duplicate_engine_instance(eng)
            new_eng.setWorkflow(call)
            profileit = lambda: new_eng.process([obj])
        else:
            profileit = lambda: call(obj, eng)
        if output:
            cProfile.runctx('profileit()', globals(), locals(), output)
        else:
            cProfile.runctx('profileit()', globals(), locals())

        if output and stats:
            for st in stats:
                fname = '%s.stats-%s' % (output, st)
                fo = open(fname, 'w')

                p = pstats.Stats(output, stream=fo)
                p.strip_dirs()
                p.sort_stats(st)
                p.print_stats()

                fo.close()
    x.__name__ = 'PROFILE'
    return x


def DEBUG_CYCLE(stmt, setup=None,
                onerror=None,
                debug_stopper=None,
                **kwargs):
    """Workflow task DEBUG_CYCLE used to repeatedly execute
    certain call - you can effectively reload modules and
    hotplug the new code, debug at runtime. The call is
    taking advantage of the internal python timeit module.
    The parameters are the same as for timeit module - i.e.

    :param stmt: string to evaluate (i.e. "print sys")
    :param setup: initialization (i.e. "import sys")

    The debug_stopper is a callback that receives (eng, obj)
    *after* execution of the main call. If debug_stopper
    returns True, it means 'stop', don't continue.

    :param onerror: string (like the setup) which will
        be appended to setup in case of error. I.e. if execution
        failed, you can reload the module and try again. This
        gets fired only after an exception!

    You can also pass any number of arguments as keywords,
    they will be available to your function at the runtime.

    Here is example of testing engine task:

    >>> from merkur.box.code import debug_cycle
    >>> def debug_stopper(obj, eng):
    ...     if obj:
    ...         return True
    ...
    >>> def engtask(config, something):
    ...     def x(obj, eng):
    ...         print(config)
    ...         print(something)
    ...     return x
    ...
    >>> config = {'some': 'value'}
    >>> debug_cycle = testpass.debug_cycle
    >>> c = DEBUG_CYCLE("engtask(config, something)(obj,eng)",
    ...                     "from __main__ import engtask",
    ...                     config=config,
    ...                     something='hi!',
    ...                     )
    >>> c('eng', 'obj')
    {'some': 'value'}
    hi!
    >>>

    You can of course test any other python calls, not only
    engine tasks with this function. If you want to reload
    code, use the setup argument:

    c = DEBUG_CYCLE("mm.engtask(config, something)(obj,eng)",
    ...                     "import mm;reload(mm)",
    ...                     config=config)

    """

    if not callable(debug_stopper):
        debug_stopper = lambda obj, eng: False
    to_pass = {}
    if kwargs:
        to_pass.update(kwargs)

    def x(obj, eng):

        storage = [0, debug_stopper, True]  # counter, callback, flag

        def _timer():
            if storage[0] == 0:
                storage[0] = 1
                return timeit.default_timer()
            else:
                storage[0] = 0
                try:
                    if storage[1](obj, eng):
                        storage[2] = False
                except:
                    traceback.print_exc()
                    storage[2] = False
                return timeit.default_timer()

        class Timer(timeit.Timer):

            def timeit(self):
                # i am taking advantage of the timeit template
                # and passing in the self object inside the array
                timing = self.inner([self], self.timer)
                return timing

        error_caught = False
        while storage[2]:
            try:
                # include passed in arguments and also obj, eng
                _setup = ';'.join(
                    ['%s=_it[0].topass[\'%s\']' % (k, k)
                     for k, v in to_pass.items()])
                _setup += '\nobj=_it[0].obj\neng=_it[0].eng'
                _setup += '\n%s' % setup
                if error_caught and onerror:
                    _setup += '\n%s' % onerror
                try:
                    t = Timer(stmt, _setup, _timer)
                except:
                    traceback.print_exc()
                    break

                # set reference to the passed in values
                t.topass = to_pass
                t.obj = obj
                t.eng = eng

                # print(t.src)
                print('Execution time: %.3s' % (t.timeit()))
            except:
                traceback.print_exc()
                lasterr = traceback.format_exc().splitlines()
                if '<timeit-src>' in lasterr[-2]:
                    sys.stderr.write(
                        'Error most likely in compilation, printing the '
                        'source code:\n%s%s\n%s\n' % (
                            '=' * 60, t.src, '=' * 60))
                    break

    x.__name__ = 'DEBUG_CYCLE'
    return x


def CALLFUNC(func, outkey=None, debug=False, stopper=None,
             args=[], oeargs=[], ekeys={}, okeys={}, **kwargs):
    """Workflow task CALLFUNC
    This wf task allows you to call any function
    :param func: identification of the function, it can be either
        string (fully qualified function name) or the callable
        itself
    :param outkey: results of the call will be stored inside
        eng.setVar(outkey) if outkey != None
    :param debug: boolean, if True, we will run the call in a
        loop, reloading the module after each error
    :param stopper: a callable which will receive obj, eng
        after each run. If the callable returns True, we will
        stop running the func (only applicable when debug=True)
    :param args: params passed on to the function
    :param ekeys: dictionary of key:value pairs, we will take
        'value' from the engine, and pass it to the function under
        the 'key' name.
    :param okeys: the same as ekeys, only that values are taken
        from the obj
    :param oeargs: definition of arguments that should be put
        inside the *args; you can use syntactic sugar to instruct
        system where to take the value, for example Eseman - will
        take eng.getVar('seman') -- 'O' [capital letter Oooo] means
        take the value from obj
    :param **kwargs: other kwargs passed on to the function
    :return: nothing, value is stored inside obj[outkey]
    """
    mod, new_func = _get_mod_func(func)
    args = list(args)

    def x(obj, eng):
        try:
            for key in oeargs:
                first_key, rest_key = key[0], key[1:]
                if first_key == 'O':
                    args.append(obj[rest_key])
                elif first_key == 'E' and eng.hasVar(rest_key):
                    args.append(eng.getVar(rest_key))
                else:
                    if key in obj:
                        args.append(obj[key])
                    elif eng.hasVar(key):
                        args.append(eng.getVar(key))
                    else:
                        raise Exception(
                            "%s is not inside obj nor eng, try specifying "
                            "Okey or Ekey" % key)
        except Exception as msg:
            eng.log.error(traceback.format_exc())
            eng.log.error(
                'Check your "oeargs" configuration. '
                'Key "%s" not available' % key)
            sys.exit(1)

        for k, v in ekeys.items():
            kwargs[k] = eng.getVar(v)
        for k, v in okeys.items():
            kwargs[k] = obj[v]

        if debug:
            universal_repeater(mod, new_func, stopper, *args, **kwargs)
        else:
            if outkey:
                obj[outkey] = new_func(*args, **kwargs)
            else:
                new_func(*args, **kwargs)
    x.__name__ = 'CALLFUNC'
    return x

# ----------------- not wf tasks -----------------------------


def _get_mod_func(func):
    """for a given callable finds its module - imports it
    and returns module, call -- module can be reloaded"""
    # find out module of this call
    def get_mod(modid, func_name):
        mod = __import__(modid)
        components = modid.split('.')
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return getattr(mod, func_name), mod

    if callable(func):
        m = func.__module__
        n = func.__name__
        new_func, mod = get_mod(m, n)
    else:
        m, n = str(func).rsplit('.', 1)
        new_func, mod = get_mod(m, n)
    return mod, new_func


def debug_simple(func, *args, **kwargs):
    """Run func with *args, **kwargs and reloads it
    after each failure - this is not a wfe task"""
    mod, new_func = _get_mod_func(func)
    universal_repeater(mod, new_func)


def universal_repeater(mod, call, stopper=None, *args, **kwargs):
    """Universal while loop."""
    fname = call.__name__
    while True:
        if callable(stopper) and stopper(*args, **kwargs):
            break
        try:
            call(*args, **kwargs)
        except:
            traceback.print_exc()
            reload(mod)
            call = getattr(mod, fname)
