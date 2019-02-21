# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2014, 2015, 2016, 2017 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Basic control flow patterns.

See http://www.yawlfoundation.org/pages/resources/patterns.html#basic
"""

import threading
import time
import collections
from functools import wraps, partial

from six.moves import _thread as thread, queue
from six import string_types

from .utils import with_nice_docs
from ..engine import Callbacks


MAX_TIMEOUT = 30000


@with_nice_docs
def TASK_JUMP_BWD(step=-1):
    """Jump to the previous task - eng.jump_call.

    Example: A, B, TASK_JUMP_FWD(-2), C, D, ...
    will produce: A, B, A, B, A, B, ... (recursion!)
    :param step: int, must not be positive number
    """
    def _move_back(obj, eng):
        eng.jump_call(step)
    _move_back.__name__ = 'TASK_JUMP_BWD'
    return _move_back


@with_nice_docs
def TASK_JUMP_FWD(step=1):
    """Jump to the next task - eng.jump_call()
    example: A, B, TASK_JUMP_FWD(2), C, D, ...
    will produce: A, B, D
    :param step: int
    """
    def _x(obj, eng):
        eng.jump_call(step)
    _x.__name__ = 'TASK_JUMP_FWD'
    return _x


@with_nice_docs
def TASK_JUMP_IF(cond, step):
    """Jump in the specified direction if the condition
    evaluates to True, the difference from other IF conditions
    is that this one does not insert the code inside a [] block
    :param cond: function
    :param step: int, negative jumps back, positive forward
    """
    def jump(obj, eng):
        return cond(obj, eng) and eng.jump_call(step)

    return jump


@with_nice_docs
def BREAK():
    """Stop execution of the current block while keeping workflow running.

    Usage: ``eng.break_current_loop()``.
    """
    def x(obj, eng):
        eng.break_current_loop()
    x.__name__ = 'BREAK'
    return x


@with_nice_docs
def STOP():
    """Unconditional stop of the workflow execution."""
    def x(obj, eng):
        eng.stopProcessing()
    x.__name__ = 'STOP'
    return x


@with_nice_docs
def HALT():
    """Unconditional stop of the workflow execution."""
    def x(obj, eng):
        eng.haltProcessing()
    x.__name__ = 'HALT'
    return x


@with_nice_docs
def OBJ_NEXT():
    """Stop the workflow execution for the current object and start
    the same worfklow for the next object - eng.break_current_loop()."""
    def x(obj, eng):
        eng.break_current_loop()
    x.__name__ = 'OBJ_NEXT'
    return x


@with_nice_docs
def OBJ_JUMP_FWD(step=1):
    """Stop the workflow execution, jumps to xth consecutive object
    and starts executing the workflow on it - eng.jumpTokenForward()
    :param step: int, relative jump from the current obj, must not be
        negative number
    """
    def x(obj, eng):
        eng.jumpTokenForward(step)
    x.__name__ = 'OBJ_JUMP_FWD'
    return x


@with_nice_docs
def OBJ_JUMP_BWD(step=-1):
    """Stop the workflow execution, jumps to xth antecedent object
    and starts executing the workflow on it - eng.jumpTokenForward()
    :param step: int, relative jump from the current obj, must not be
        negative number
    """
    def _x(obj, eng):
        eng.jumpTokenBackward(step)
    _x.__name__ = 'OBJ_JUMP_BWD'
    return _x

# ------------------------- some conditions --------------------------------- #


@with_nice_docs
def IF(cond, branch):
    """Implement condition, if cond evaluates to True branch is executed.

    :param cond: callable, function that decides
    :param branch: block of functions to run

    @attention: the branch is inserted inside [] block, therefore jumping is
                limited only inside the branch
    """
    def _x(obj, eng):
        return cond(obj, eng) and eng.jump_call(1) \
            or eng.break_current_loop()
    _x.__name__ = 'IF'
    return [_x, branch]


@with_nice_docs
def IF_NOT(cond, branch):
    """Implements condition, if cond evaluates to False
    branch is executed
    :param cond: callable, function that decides
    :param branch: block of functions to run

    @attention: the branch is inserted inside [] block, therefore jumping is
                limited only inside the branch
    """
    def _x(obj, eng):
        if cond(obj, eng):
            eng.break_current_loop()
        return 1
    _x.__name__ = 'IF_NOT'
    return [_x, branch]


@with_nice_docs
def IF_ELSE(cond, branch1, branch2):
    """Implements condition, if cond evaluates to True
    branch1 is executed, otherwise branch2
    :param cond: callable, function that decides
    :param branch1: block of functions to run [if=true]
    :param branch2: block of functions to run [else]

    @attention: the branch is inserted inside [] block, therefore jumping is
                limited only inside the branch
    """
    if branch1 is None or branch2 is None:
        raise Exception("Neither of the branches can be None/empty")

    def _x(obj, eng):
        return cond(obj, eng) and eng.jump_call(1) \
            or eng.jump_call(3)
    _x.__name__ = 'IF_ELSE'
    return [_x, branch1, BREAK(), branch2]


@with_nice_docs
def WHILE(cond, branch):
    """Keeps executing branch as long as the condition cond is True
    :param cond: callable, function that decides
    :param branch: block of functions to run [if=true]
    """
    # quite often i passed a function, which results in errors
    if callable(branch):
        branch = (branch,)
    # we don't know what is hiding inside branch
    branch = tuple(Callbacks.cleanup_callables(branch))

    def _x(obj, eng):
        if not cond(obj, eng):
            eng.break_current_loop()
    _x.__name__ = 'WHILE'
    return [_x, branch, TASK_JUMP_BWD(-(len(branch) + 1))]


@with_nice_docs
def CMP(a, b, op):
    """Task that can be used in if or something else to compare two values.

    :param a: left-hand-side value
    :param b: right-hand-side value
    :param op: Operator can be :
        eq , gt , gte , lt , lte
        == , >  , >=  , <  , <=
    :return: bool: result of the test
    """
    @wraps(CMP)
    def _CMP(obj, eng):
        a_ = a
        b_ = b
        while callable(a_):
            a_ = a_(obj, eng)
        while callable(b_):
            b_ = b_(obj, eng)
        return {
            "eq": lambda a_, b_: a_ == b_,
            "gt": lambda a_, b_: a_ > b_,
            "gte": lambda a_, b_: a_ >= b_,
            "lt": lambda a_, b_: a_ < b_,
            "lte": lambda a_, b_: a_ <= b_,

            "==": lambda a_, b_: a_ == b_,
            ">": lambda a_, b_: a_ > b_,
            ">=": lambda a_, b_: a_ >= b_,
            "<": lambda a_, b_: a_ < b_,
            "<=": lambda a_, b_: a_ <= b_,
            "in": lambda a_, b_: b_ in a_,
        }[op](a_, b_)
    _CMP.hide = True
    return _CMP


def _setter(key, obj, eng, step, val):
    eng.extra_data[key] = val


@with_nice_docs
def FOR(get_list_function, setter, branch, cache_data=False, order="ASC"):
    """For loop that stores the current item.
    :param get_list_function: function returning the list on which we should
    iterate.
    :param branch: block of functions to run
    :param savename: name of variable to save the current loop state in the
    extra_data in case you want to reuse the value somewhere in a task.
    :param cache_data: can be True or False in case of True, the list will be
    cached in memory instead of being recomputed everytime. In case of caching
    the list is no more dynamic.
    :param order: because we should iterate over a list you can choose in which
    order you want to iterate over your list from start to end(ASC) or from end
    to start (DSC).
    :param setter: function to call in order to save the current item of the
    list that is being iterated over.
    expected to take arguments (obj, eng, val)
    :param getter: function to call in order to retrieve the current item of
    the list that is being iterated over. expected to take arguments(obj, eng)
    """
    # be sane
    assert order in ('ASC', 'DSC')
    # sanitize string better
    if isinstance(setter, string_types):
        setter = partial(_setter, setter)
    # quite often i passed a function, which results in errors
    if callable(branch):
        branch = (branch,)
    # we don't know what is hiding inside branch
    branch = tuple(Callbacks.cleanup_callables(branch))

    def _for(obj, eng):
        step = str(eng.getCurrTaskId())  # eg '[1]'
        if "_Iterators" not in eng.extra_data:
            eng.extra_data["_Iterators"] = {}

        def get_list():
            try:
                return eng.extra_data["_Iterators"][step]["cache"]
            except KeyError:
                if callable(get_list_function):
                    return get_list()
                elif isinstance(get_list_function, collections.Iterable):
                    return list(get_list_function)
                else:
                    raise TypeError("get_list_function is not a callable nor a"
                                    " iterable")

        my_list_to_process = get_list()

        # First time we are in this step
        if step not in eng.extra_data["_Iterators"]:
            eng.extra_data["_Iterators"][step] = {}
            # Cache list
            if cache_data:
                eng.extra_data["_Iterators"][step]["cache"] = get_list()
            # Initialize step value
            eng.extra_data["_Iterators"][step]["value"] = {
                "ASC": 0,
                "DSC": len(my_list_to_process) - 1}[order]
            # Store previous data
            if 'current_data' in eng.extra_data["_Iterators"][step]:
                eng.extra_data["_Iterators"][step]["previous_data"] = \
                    eng.extra_data["_Iterators"][step]["current_data"]

        # Increment or decrement step value
        step_value = eng.extra_data["_Iterators"][step]["value"]
        currently_within_list_bounds = \
            (order == "ASC" and step_value < len(my_list_to_process)) or \
            (order == "DSC" and step_value > -1)
        if currently_within_list_bounds:
            # Store current data for ourselves
            eng.extra_data["_Iterators"][step]["current_data"] = \
                my_list_to_process[step_value]
            # Store for the user
            if setter:
                setter(obj, eng, step, my_list_to_process[step_value])
            if order == 'ASC':
                eng.extra_data["_Iterators"][step]["value"] += 1
            elif order == 'DSC':
                eng.extra_data["_Iterators"][step]["value"] -= 1
        else:
            setter(obj, eng, step,
                   eng.extra_data["_Iterators"][step]["previous_data"])
            del eng.extra_data["_Iterators"][step]
            eng.break_current_loop()

    _for.__name__ = 'FOR'
    return [_for, branch, TASK_JUMP_BWD(-(len(branch) + 1))]


@with_nice_docs
def PARALLEL_SPLIT(*args):
    """Start task in parallel.

    @attention: tasks A,B,C,D... are not addressable, you can't
        you can't use jumping to them (they are invisible to
        the workflow engine). Though you can jump inside the
        branches
    @attention: tasks B,C,D... will be running on their own
        once you have started them, and we are not waiting for
        them to finish. Workflow will continue executing other
        tasks while B,C,D... might be still running.
    @attention: a new engine is spawned for each branch or code,
        all operations works as expected, but mind that the branches
        know about themselves, they don't see other tasks outside.
        They are passed the object, but not the old workflow
        engine object
    @postcondition: eng object will contain lock (to be used
        by threads)
    """
    def _parallel_split(obj, eng, calls):
        lock = thread.allocate_lock()
        eng.store['lock'] = lock
        for func in calls:
            new_eng = eng.duplicate()
            new_eng.setWorkflow(
                [lambda o, e: e.store.update({'lock': lock}), func]
            )
            thread.start_new_thread(new_eng.process, ([obj], ))
            # new_eng.process([obj])
    return lambda o, e: _parallel_split(o, e, args)


@with_nice_docs
def SYNCHRONIZE(*args, **kwargs):
    """
    After the execution of task B, task C, and task D, task E can be executed.
    :param *args: args can be a mix of callables and list of callables
        the simplest situation comes when you pass a list of callables
        they will be simply executed in parallel.
        But if you pass a list of callables (branch of callables)
        which is potentionally a new workflow, we will first create a
        workflow engine with the workflows, and execute the branch in it
    @attention: you should never jump out of the synchronized branches
    """
    timeout = MAX_TIMEOUT
    if 'timeout' in kwargs:
        timeout = kwargs['timeout']

    if len(args) < 2:
        raise Exception('You must pass at least two callables')

    def _synchronize(obj, eng):
        queue = MyTimeoutQueue()
        # spawn a pool of threads, and pass them queue instance
        for i in range(len(args) - 1):
            t = MySpecialThread(queue)
            t.setDaemon(True)
            t.start()

        for func in args[0:-1]:
            if isinstance(func, list) or isinstance(func, tuple):
                new_eng = eng.duplicate()
                new_eng.setWorkflow(func)
                queue.put(lambda: new_eng.process([obj]))
            else:
                queue.put(lambda: func(obj, eng))

        # wait on the queue until everything has been processed
        queue.join_with_timeout(timeout)

        # run the last func
        args[-1](obj, eng)
    _synchronize.__name__ = 'SYNCHRONIZE'
    return _synchronize


@with_nice_docs
def CHOICE(arbiter, *predicates, **kwpredicates):
    """
    A choice is made to execute either task B, task C or task D
    after execution of task A.
    :param arbiter: a function which returns some value (the value
        must be inside the predicates dictionary)
    :param predicates: list of callables, the first item must be the
        value returned by the arbiter, example:
        ('submit', task_a),
        ('upload' : task_a, [task_b, task_c]...)
    :param **kwpredicates: you can supply predicates also as a
        keywords, example
        CHOICE(arbiter, one=lambda...., two=[lambda o,e:...., ...])
    @postcondition: all tasks are 'jumpable'

    """
    workflow = []
    mapping = {}
    for branch in predicates:
        workflow.append(branch[1:])
        mapping[branch[0]] = len(workflow)
        workflow.append(BREAK())

    for k, v in kwpredicates.items():
        workflow.append(v)
        mapping[k] = len(workflow)
        workflow.append(BREAK())

    def _exclusive_choice(obj, eng):
        val = arbiter(obj, eng)
        i = mapping[val]  # die on error
        eng.jump_call(i)
    c = _exclusive_choice
    c.__name__ = arbiter.__name__
    workflow.insert(0, c)
    return workflow


@with_nice_docs
def SIMPLE_MERGE(*args):
    """
    Task E will be started when any one of the tasks B, C or D completes.
    This pattern though makes a context assumption: there is no
    parallelism preceding task E.
    """

    if len(args) < 2:
        raise Exception("You must suply at least 2 callables")

    final_task = args[-1]
    workflow = []
    total = ((len(args) - 1) * 2) + 1
    for branch in args[0:-1]:
        total -= 2
        workflow.append(branch)
        workflow.append(TASK_JUMP_FWD(total))

    workflow.append(final_task)
    return workflow


class MyTimeoutQueue(queue.Queue):

    def join_with_timeout(self, timeout):
        self.all_tasks_done.acquire()
        try:
            endtime = time.time() + timeout
            while self.unfinished_tasks:
                remaining = endtime - time.time()
                if remaining <= 0.0:
                    raise threading.ThreadError('NotFinished')
                time.sleep(.05)
                self.all_tasks_done.wait(remaining)
        finally:
            self.all_tasks_done.release()


class MySpecialThread(threading.Thread):

    def __init__(self, itemq, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.itemq = itemq

    def run(self):
        call = self.itemq.get()
        call()
