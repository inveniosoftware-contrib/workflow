# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2012, 2014, 2015, 2016 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.


"""Contains standard error messages for workflows module."""

import sys
from functools import wraps, partial
from types import MethodType


def with_str(args):
    """Attach bound method `__str__` to a class instance.

    :type args: list or tuple
    :param args:
        args[0], is taken from `self` and printed
        args[1], if exists, is iterated over and each element is looked in
        `self` and printed in a `key=value` format.

    Usage example:
    ..code-block:: python
        @with_str(('message', ('foo', 'bar')))
        class MyException(Exception):
            def __init__(self, message, a, b):
                self.message = message
                self.foo = a
                self.bar = b

        >> print(MyException('hello', 1, 2))
        >> MyException(hello, foo=1, bar=2),
    """
    @wraps(with_str)
    def wrapper(Klass):
        def __str__(args, self):
            """String representation."""
            real_args = getattr(self, args[0])
            try:
                real_kwargs = {}
                for key in args[1]:
                    real_kwargs[key] = getattr(self, key)
            except IndexError:
                real_kwargs = {}
            return "{class_name}({real_args}, {real_kwargs})".format(
                class_name=Klass.__name__,
                real_args=real_args,
                real_kwargs=', '.join(('{k}={v}'.format(k=key, v=val)
                                       for key, val in real_kwargs.items())))
        if sys.version_info >= (3, ):
            Klass.__str__ = MethodType(partial(__str__, args), Klass)
        else:
            Klass.__str__ = MethodType(partial(__str__, args), None, Klass)
        return Klass
    return wrapper


class WorkflowTransition(Exception):
    """Base class for workflow exceptions."""


class StopProcessing(WorkflowTransition):
    """Stop current workflow."""


@with_str(('message', ('action', 'payload')))
class HaltProcessing(WorkflowTransition):  # Used to be WorkflowHalt
    """Halt the workflow (can be used for nested workflow engines).

    Also contains the widget and other information to be displayed.
    """
    def __init__(self, message="", action=None, payload=None):
        """Instanciate a HaltProcessing object."""
        super(HaltProcessing, self).__init__()
        self.message = message
        self.action = action
        self.payload = payload


class ContinueNextToken(WorkflowTransition):
    """Jump up to next token (it can be called many levels deep)."""


class JumpToken(WorkflowTransition):
    """Jump N steps in the given direction."""


class JumpTokenForward(WorkflowTransition):
    """Jump N steps forwards."""


class JumpTokenBack(WorkflowTransition):
    """Jump N steps back."""


class JumpCall(WorkflowTransition):
    """In one loop ``[call, call...]``, jump `x` steps."""


# Deprecated
class JumpCallForward(WorkflowTransition):
    """In one loop ``[call, call...]``, jump `x` steps forward."""


# Deprecated
class JumpCallBack(WorkflowTransition):
    """In one loop ``[call, call...]``, jump `x` steps forward."""


class BreakFromThisLoop(WorkflowTransition):
    """Break from this loop, but do not stop processing."""


@with_str(('message', ('id_workflow', 'id_object', 'payload')))
class WorkflowError(Exception):
    """Raised when workflow experiences an error."""

    def __init__(self, message, id_workflow=None,
                 id_object=None, payload=None):
        """Instanciate a WorkflowError object."""
        self.message = message
        self.id_workflow = id_workflow
        self.id_object = id_object
        self.payload = payload
        # Needed for passing an exception through message queue
        super(WorkflowError, self).__init__(message)


@with_str(('message', ('workflow_name', 'payload')))
class WorkflowDefinitionError(Exception):
    """Raised when workflow definition is missing."""

    def __init__(self, message, workflow_name, payload=None):
        """Instanciate a WorkflowDefinitionError object."""
        self.message = message
        self.workflow_name = workflow_name
        self.payload = payload
        super(WorkflowDefinitionError, self).__init__(message, workflow_name,
                                                      payload)


@with_str(('message', ('obj_status', 'id_object')))
class WorkflowObjectStatusError(Exception):
    """Raised when workflow object has an unknown or missing version."""

    def __init__(self, message, id_object, obj_status):
        """Instanciate a WorkflowObjectStatusError object."""
        self.message = message
        self.obj_status = obj_status
        self.id_object = id_object


class WorkflowAPIError(Exception):
    """Raised when there is a problem with parameters at the API level."""


class SkipToken(WorkflowTransition):
    """Used by workflow engine to skip the current process of an object."""


class AbortProcessing(WorkflowTransition):
    """Used by workflow engine to abort the engine execution."""
