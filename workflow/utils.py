# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2012, 2014, 2015 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.


class dictproperty(object):
    """Use a dict attribute as a @property.

    This is a minimal descriptor class that creates a proxy object,
    which implements __getitem__, __setitem__ and __delitem__,
    passing requests through to the functions that the user
    provided to the dictproperty constructor.
    """

    class _proxy(object):

        """The proxy object."""

        def __init__(self, obj, fget, fset, fdel):
            """Init the proxy object."""
            self._obj = obj
            self._fget = fget
            self._fset = fset
            self._fdel = fdel

        def __getitem__(self, key):
            """Get value from key."""
            return self._fget(self._obj, key)

        def __setitem__(self, key, value):
            """Set value for key."""
            self._fset(self._obj, key, value)

        def __delitem__(self, key):
            """Delete value for key."""
            self._fdel(self._obj, key)

    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        """Init descriptor class."""
        self._fget = fget
        self._fset = fset
        self._fdel = fdel
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        """Return proxy or self."""
        if obj is None:
            return self
        return self._proxy(obj, self._fget, self._fset, self._fdel)

def get_func_info(func):
    """Retrieve a function's information."""
    name = func.func_name
    doc = func.func_doc
    try:
        nicename = func.description
    except AttributeError:
        if doc:
            nicename = doc.split('\n')[0]
            if len(nicename) > 80:
                nicename = name
        else:
            nicename = name
    parameters = []
    closure = func.func_closure
    varnames = func.func_code.co_freevars
    if closure:
        for index, arg in enumerate(closure):
            parameters.append((str(varnames[index]), str(arg.cell_contents)))
    return {
        "nicename": nicename,
        "doc": doc,
        "parameters": parameters,
        "name": name
    }


# https://mail.python.org/pipermail/python-ideas/2011-January/008958.html
class staticproperty(object):

    """Property decorator for static methods."""

    def __init__(self, function):
        self._function = function

    def __get__(self, instance, owner):
        return self._function()


class classproperty(object):

    """Property decorator for class methods."""

    def __init__(self, function):
        self._function = function

    def __get__(self, instance, owner):
        return self._function(owner)
