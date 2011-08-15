# -*- coding: utf8 -*-
import unittest
import sys
import os

p = os.path.abspath(os.path.dirname(__file__) + '/../')
if p not in sys.path:
    sys.path.append(p)

from workflow.engine import GenericWorkflowEngine, PhoenixWorkflowEngine

import unittest_engine_interface
import unittest_engine_workflow
import unittest_patterns


import copy
import threading
import pickle

from cloud import serialization

class TestPhoenixWorkflowEngine(PhoenixWorkflowEngine):

    @staticmethod
    def before_processing(objects, self):
        """Standard pre-processing callback - saves a pointer to the processed objects"""
        #self.reset()
        self._objects = objects
        self._original_objs = copy.deepcopy(objects)


    @staticmethod
    def after_processing(objects, self):
        """Standard post-processing callback, basic cleaning"""
        self._objects = []
        self._i = [0]

        main_thread = threading.current_thread().name == 'MainThread'

        if not main_thread:
            return

        # get a deepcopy of the original objects
        orig_objs = self._original_objs

        #create a new wfe (but make sure we don't call serialization again)
        wfe2 = pickle.dumps(self)
        assert isinstance(wfe2, basestring)
        wfe2 = pickle.loads(wfe2)
        wfe2.after_processing = lambda objs,eng: []

        # test if the results are identical
        wfe2.process(orig_objs)
        s1 = str(objects)
        s2 = str(orig_objs)

        if not self.getVar('lock'):
            assert s1 == s2
            assert orig_objs == objects
        else:
            print 'WFE executed threads, results may be different'
            print 'original result:', objects
            print 're-executed res:', orig_objs




def suite():
    unittest_engine_interface.GenericWorkflowEngine = PhoenixWorkflowEngine
    unittest_engine_workflow.GenericWorkflowEngine = PhoenixWorkflowEngine
    unittest_patterns.GenericWorkflowEngine = PhoenixWorkflowEngine

    suite = unittest.TestSuite()
    #suite.addTest(WorkflowEngine('test_workflow'))
    suite.addTest(unittest.makeSuite(unittest_engine_interface.TestGenericWorkflowEngine))
    suite.addTest(unittest.makeSuite(unittest_engine_workflow.TestWorkflowEngine))
    suite.addTest(unittest.makeSuite(unittest_patterns.TestGenericWorkflowEngine))
    return suite

if __name__ == '__main__':
    #unittest.main()
    unittest.TextTestRunner(verbosity=2).run(suite())
