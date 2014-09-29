# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Workflow patterns definitions."""

# basic flow commands
from .controlflow import (STOP, BREAK, OBJ_JUMP_BWD, OBJ_JUMP_FWD, OBJ_NEXT,
                          TASK_JUMP_BWD, TASK_JUMP_FWD, TASK_JUMP_IF)


# conditions
from .controlflow import IF, IF_NOT, IF_ELSE, WHILE

# basic patterns
from .controlflow import PARALLEL_SPLIT, SYNCHRONIZE, SIMPLE_MERGE, CHOICE


# helper functions
from .utils import (EMPTY_CALL, ENG_GET, ENG_SET, OBJ_SET, OBJ_GET, ERROR, TRY,
                    RUN_WF, CALLFUNC, DEBUG_CYCLE, PROFILE)
