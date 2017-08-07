# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2014, 2016 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""
Version information for workflow package.

This file is imported by ``workflow.__init__``, and parsed by
``setup.py`` as well as ``docs/conf.py``.
"""
import autosemver


__version__ = autosemver.packaging.get_current_version(
    project_name='workflow'
)
