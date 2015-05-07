# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2012, 2014, 2015 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.


"""Contain signals emitted from workflows module.

Import with care. It is not neccessary that `blinker` is available."""

from blinker import Namespace
_signals = Namespace()


workflow_halted = _signals.signal('workflow_halted')
"""
This signal is sent when a workflow engine's halt function is called.
Sender is the bibworkflow object that was running before the workflow
was halted.
"""

workflow_started = _signals.signal('workflow_started')
"""
This signal is sent when a workflow is started.
Sender is the workflow engine object running the workflow.
"""

workflow_finished = _signals.signal('workflow_finished')
"""
This signal is sent when a workflow is finished.
Sender is the workflow engine object running the workflow.
"""

workflow_error = _signals.signal('workflow_error')
"""
This signal is sent when a workflow object gets an error.
Sender is the workflow engine object that was running before the workflow
got the error.
"""
