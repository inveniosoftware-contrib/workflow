# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

import unittest
import sys
import os

p = os.path.abspath(os.path.dirname(__file__) + '/../')
if p not in sys.path:
    sys.path.append(p)

from workflow.engine import GenericWorkflowEngine


def m(key):
    def _m(obj, eng):
        obj.append(key)
    return _m


def stop_if_str(value):
    def x(obj, eng):
        if str(obj) == value:
            eng.stopProcessing()
    return lambda obj, eng: x(obj, eng)


def asterisk_chooser(obj, eng):
    return eng.getCallbacks('*')


def empty_chooser(obj, eng):
    return []


def call_forward(step=0):
    return lambda obj, eng: eng.jumpCallForward(step)


class TestGenericWorkflowEngine(unittest.TestCase):

    """Tests of the WE interface"""

    def setUp(self):
        self.key = '*'

    def tearDown(self):
        pass

    def getDoc(self, val=None):
        if val:
            return [[x] for x in val.split()]
        return [[x] for x in u"one two three four five".split()]

    def addTestCallbacks(self, no, eng):
        if type == 1:
            eng.addManyCallbacks()

    # --------- initialization ---------------

    def test_init(self):
        d1 = self.getDoc()
        d2 = self.getDoc()
        d3 = self.getDoc()

        # init with empty to full parameters
        we1 = GenericWorkflowEngine()
        we2 = GenericWorkflowEngine(callback_chooser=asterisk_chooser)

        try:
            we3 = GenericWorkflowEngine(processing_factory='x',
                                        callback_chooser='x',
                                        before_processing='x',
                                        after_processing='x')
        except Exception as msg:
            assert 'must be a callable' in str(msg)

        try:
            we3 = GenericWorkflowEngine(callback_chooser=asterisk_chooser,
                                        after_processing='x')
        except Exception as msg:
            assert 'must be a callable' in str(msg)

        we1.addManyCallbacks('*', [
            m('mouse'),
            [m('dog'), call_forward(1), m('cat'), m('puppy')],
            m('horse'),
        ])
        we2.addManyCallbacks('*', [
            m('mouse'),
            [m('dog'), call_forward(1), m('cat'), m('puppy')],
            m('horse'),
        ])

        we1.process(d1)
        we2.process(d2)

    def test_configure(self):

        d1 = self.getDoc()
        d2 = self.getDoc()
        d3 = self.getDoc()

        we = GenericWorkflowEngine()
        we.addManyCallbacks('*', [
            m('mouse'),
            [m('dog'), call_forward(1), m('cat'), m('puppy')],
            m('horse'),
        ])

        # process using defaults
        we.process(d1)
        r = 'one mouse dog cat puppy horse'.split()

        # pass our own callback chooser
        we.configure(callback_chooser=asterisk_chooser)
        we.process(d2)

        assert d1[0] == r
        assert d2[0] == r
        assert d1 == d2

        # configure it wrongly
        we.configure(callback_chooser='')

        self.failUnlessRaises(Exception, we.process, d3)

        assert d3 == self.getDoc()

    # ------------ tests configuring the we --------------------
    def test_workflow01(self):
        we0 = GenericWorkflowEngine()
        we1 = GenericWorkflowEngine()
        we2 = GenericWorkflowEngine()

        d0 = self.getDoc()
        d1 = self.getDoc()
        d2 = self.getDoc()

        we0.addManyCallbacks('*', [
            m('mouse'),
            [m('dog'), call_forward(1), m('cat'), m('puppy')],
            m('horse'),
        ])
        we1.setWorkflow([
            m('mouse'),
            [m('dog'), call_forward(1), m('cat'), m('puppy')],
            m('horse'),
        ])
        we2.addManyCallbacks('x', [
            m('mouse'),
            [m('dog'), call_forward(1), m('cat'), m('puppy')],
            m('horse'),
        ])
        we2.configure(callback_chooser=lambda o, e: e.getCallbacks('x'))

        we0.process(d0)
        we1.process(d1)
        we2.process(d2)

        assert d0 == d1
        assert d0 == d2


def suite():
    suite = unittest.TestSuite()
    # suite.addTest(WorkfloGenericWorkflowEngine('test_workflow'))
    suite.addTest(unittest.makeSuite(TestGenericWorkflowEngine))
    return suite

if __name__ == '__main__':
    # unittest.main()
    unittest.TextTestRunner(verbosity=2).run(suite())
