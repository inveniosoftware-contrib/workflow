# -*- coding: utf-8 -*-
# This file is part of Invenio.
# Copyright (C) 2013, 2014, 2015, 2016 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""Deprecation warning related helpers."""
import warnings
from functools import wraps


# Improved version of http://code.activestate.com/recipes/391367-deprecated/
def deprecated(message, category=DeprecationWarning):
    def wrap(func):
        """Decorator which can be used to mark functions as deprecated.

        :param message: text to include in the warning
        :param category: warning category exception class
        """
        @wraps(func)
        def new_func(*args, **kwargs):
            warnings.warn(message, category, stacklevel=3)
            return func(*args, **kwargs)
        return new_func
    return wrap
