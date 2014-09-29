# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

from __future__ import print_function

import unittest
import six
import sys
import os

p = os.path.abspath(os.path.dirname(__file__) + '/../')
if p not in sys.path:
    sys.path.append(p)

from workflow.engine import GenericWorkflowEngine, PhoenixWorkflowEngine

import test_engine_interface
import test_engine_workflow
import test_patterns


import copy
import threading
import pickle


class TestPhoenixWorkflowEngine(PhoenixWorkflowEngine):

    @staticmethod
    def before_processing(objects, self):
        """Saves a pointer to the processed objects."""
        # self.reset()
        self._objects = objects
        self._original_objs = copy.deepcopy(objects)

    @staticmethod
    def after_processing(objects, self):
        """Standard post-processing callback, basic cleaning."""
        self._objects = []
        self._i = [0]

        main_thread = threading.current_thread().name == 'MainThread'

        if not main_thread:
            return

        # get a deepcopy of the original objects
        orig_objs = self._original_objs

        # create a new wfe (but make sure we don't call serialization again)
        wfe2 = pickle.dumps(self)
        assert isinstance(wfe2, six.string_types)
        wfe2 = pickle.loads(wfe2)
        wfe2.after_processing = lambda objs, eng: []

        # test if the results are identical
        wfe2.process(orig_objs)
        s1 = str(objects)
        s2 = str(orig_objs)

        if not self.getVar('lock'):
            assert s1 == s2
            assert orig_objs == objects
        else:
            print('WFE executed threads, results may be different')
            print('original result:', objects)
            print('re-executed res:', orig_objs)


def suite():
    test_engine_interface.GenericWorkflowEngine = PhoenixWorkflowEngine
    test_engine_workflow.GenericWorkflowEngine = PhoenixWorkflowEngine
    test_patterns.GenericWorkflowEngine = PhoenixWorkflowEngine

    suite = unittest.TestSuite()
    # suite.addTest(WorkflowEngine('test_workflow'))
    suite.addTest(
        unittest.makeSuite(test_engine_interface.TestGenericWorkflowEngine))
    suite.addTest(unittest.makeSuite(test_engine_workflow.TestWorkflowEngine))
    suite.addTest(unittest.makeSuite(test_patterns.TestGenericWorkflowEngine))
    return suite

if __name__ == '__main__':
    # unittest.main()
    unittest.TextTestRunner(verbosity=2).run(suite())
