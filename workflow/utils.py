# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2012, 2014, 2015, 2016 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.


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
