# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

import unittest
import six
import sys
import os

p = os.path.abspath(os.path.dirname(__file__) + '/../')
if p not in sys.path:
    sys.path.append(p)

from workflow.engine import GenericWorkflowEngine, HaltProcessing

wfe_impl = GenericWorkflowEngine


def m(key=None):
    def _m(token, inst):
        token.setFeatureKw(
            sem=((token.getFeature('sem') or '') + ' ' + key).strip())
    return _m


def if_str_token_back(value='', step=0):
    def x(token, inst):
        if str(token) == value and not token.getFeature('token_back'):
            token.setFeature('token_back', 1)
            inst.jumpTokenBack(step)
    return lambda token, inst: x(token, inst)


def if_str_token_forward(value='', step=0):
    def x(token, inst):
        if str(token) == value and not token.getFeature('token_forward'):
            token.setFeature('token_forward', 1)
            inst.jumpTokenForward(step)
    return lambda token, inst: x(token, inst)


def call_back(step=0):
    def x(token, inst):
        if not token.getFeature('back'):
            token.setFeature('back', 1)
            inst.jumpCallBack(step)
    return lambda token, inst: x(token, inst)


def call_forward(step=0):
    return lambda token, inst: inst.jumpCallForward(step)


def break_loop():
    return lambda token, inst: inst.breakFromThisLoop()


def stop_processing():
    return lambda token, inst: inst.stopProcessing()


def halt_processing():
    return lambda token, inst: inst.haltProcessing()


def next_token():
    return lambda token, inst: inst.continueNextToken()


def get_first(doc):
    return doc[0].getFeature('sem')


def get_xth(doc, xth):
    return doc[xth].getFeature('sem')


def stop_if_str(value=None):
    def x(token, inst):
        if str(token) == value:
            inst.stopProcessing()
    return lambda token, inst: x(token, inst)


class FakeToken(object):

    def __init__(self, data, **attributes):
        self.data = data
        self.pos = None  # set TokenCollection on obj return
        # here link to TokenCollection (when returning)
        self.backreference = None
        self.__prev = 0
        self.__next = 0
        self.__attributes = {}
        for attr_name, attr_value in attributes.items():
            self.setFeature(attr_name, attr_value)

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return 'Token(%s, **%s)' % (repr(self.data), repr(self.__attributes))

    def __eq__(self, y):
        return self.data == y

    def __ne__(self, y):
        return self.data != y

    def __get(self, index):
        if self.backreference is None:
            raise Exception("No collection available")
        backr = self.backreference()
        if index < len(backr) and index >= 0:
            return backr[index]
        else:
            return None

    def neighbour(self, index):
        return self.__get(self.pos + index)

    def reset(self):
        self.__prev = 0
        self.__next = 0

    def prev(self):
        self.__prev += 1
        return self.__get(self.pos - self.__prev)

    def next(self):
        self.__next += 1
        return self.__get(self.pos + self.__next)

    def isa(self, *args, **kwargs):
        if args and self.data != args[0]:
            return False
        for key, value in kwargs.items():
            if self.getFeature(key) != value:
                return False
        return True

    def setValue(self, value):
        self.data = value

    def getFeature(self, key):
        try:
            return self.__attributes[key]
        except KeyError:
            return None

    def setFeature(self, key, value):
        self.__attributes[key] = value

    def setFeatureKw(self, **kwargs):
        for key, value in kwargs.items():
            self.setFeature(key, value)

    def getAllFeatures(self):
        return self.__attributes


class TestWorkflowEngine(unittest.TestCase):

    """Tests using FakeTokens in place of strings"""

    def setUp(self):
        self.key = '*'
        self.we = wfe_impl()
        self.data = "one\ntwo\nthree\nfour\nfive"
        self.doc = [FakeToken(x, type='*') for x in self.data.splitlines()]

    def tearDown(self):
        pass

    # --------- call_forward ---------------

    def test_workflow_01(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), call_forward(1), m('cat'), m('puppy')],
            m('horse'),
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'mouse dog cat puppy horse'

    def test_workflow_02(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), call_forward(2), m('cat'), m('puppy'), m('python')],
            m('horse'),
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'mouse dog puppy python horse'

    def test_workflow_03(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), call_forward(50), m('cat'), m('puppy'), m('python')],
            m('horse'),
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'mouse dog horse'

    def test_workflow_04(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), call_forward(2), m('cat'), m('puppy'), m('python')],
            m('horse'),
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'mouse dog puppy python horse'

    def test_workflow_05(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), call_forward(-2), m('cat'), m('puppy'), m('python')],
            m('horse'),
        ])
        doc = self.doc
        try:
            self.we.process(doc)
        except:
            t = get_first(doc)
            assert t == 'mouse dog'
        else:
            raise Exception("call_forward allowed negative number")

    def test_workflow_06(self):
        self.we.addManyCallbacks(self.key, [
            call_forward(3),
            m('mouse'),
            [m('dog'), call_forward(2), m('cat'), m('puppy'), m('python')],
            m('horse'),
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'horse'

    # ------------- call_back -------------------

    def test_workflow_01b(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), call_back(-1), m('cat'), m('puppy')],
            m('horse'),
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'mouse dog dog cat puppy horse'

    def test_workflow_02b(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), m('cat'), m('puppy'), m('python'), call_back(-2)],
            m('horse'),
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'mouse dog cat puppy python puppy python horse'

    def test_workflow_03b(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), m('cat'), call_back(-50), m('puppy'), m('python')],
            m('horse'),
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'mouse dog cat dog cat puppy python horse'

    def test_workflow_04b(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), m('cat'), m('puppy'), m('python')],
            m('horse'),
            call_back(-2)
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == ('mouse dog cat puppy python horse '
                     'dog cat puppy python horse')

    def test_workflow_05b(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'), call_back(2), m('cat'), m('puppy'), m('python')],
            m('horse'),
        ])
        doc = self.doc
        try:
            self.we.process(doc)
        except:
            t = get_first(doc)
            assert t == 'mouse dog'
        else:
            raise Exception("call_back allowed positive number")

    # --------- complicated loop -----------

    def test_workflow_07(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'),
              [m('wasp'), m('leon')],
              ],
             m('horse'),
             ]
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'mouse dog cat puppy python wasp leon horse'

    def test_workflow_07a(self):
        self.we.addManyCallbacks(self.key, [
            call_forward(2),
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'), call_back(-2),
              [m('wasp'), m('leon')],
              ],
             m('horse'),
             ]
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'dog cat puppy python python wasp leon horse'

    # ----------- BreakFromThisLoop -----------

    def test_workflow_07b(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'), break_loop(),
              [m('wasp'), m('leon')],
              ],
             m('horse'),
             ]
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t == 'mouse dog cat puppy python horse'

    def test_workflow_07c(self):
        self.we.addManyCallbacks(self.key, [
            break_loop(),
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'),
              [m('wasp'), m('leon')],
              ],
             m('horse'),
             ]
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert t is None

    # ----------- processing of a whole collection --------

    # ----------- StopProcessing --------------------------

    def test_workflow_08(self):
        self.we.addManyCallbacks(self.key, [
            stop_processing(),
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'),
              [m('wasp'), m('leon')],
              ],
             m('horse'),
             ]
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        assert get_xth(doc, 0) is None
        assert get_xth(doc, 1) is None
        assert get_xth(doc, 2) is None
        assert str(doc[0]) == 'one'
        assert str(doc[1]) == 'two'
        assert str(doc[2]) == 'three'

    def test_workflow_08a(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'),
              stop_if_str('four'),
              [m('wasp'), m('leon')],
              ],
             m('horse'),
             ]
        ])
        doc = self.doc
        self.we.process(doc)
        r1 = 'mouse dog cat puppy python wasp leon horse'
        r2 = 'mouse dog cat puppy python'
        assert get_xth(doc, 0) == r1
        assert get_xth(doc, 1) == r1
        assert get_xth(doc, 2) == r1
        assert get_xth(doc, 3) == r2
        assert get_xth(doc, 4) is None
        assert str(doc[0]) == 'one'
        assert str(doc[1]) == 'two'
        assert str(doc[2]) == 'three'
        assert str(doc[3]) == 'four'
        assert str(doc[4]) == 'five'

    # ---------- jumpTokenNext -------------

    def test_workflow_09(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'),
              next_token(),
              [m('wasp'), m('leon')],
              ],
             m('horse'),
             ]
        ])
        doc = self.doc
        self.we.process(doc)
        r1 = 'mouse dog cat puppy python'
        r2 = 'mouse dog cat puppy python'
        assert get_xth(doc, 0) == r1
        assert get_xth(doc, 1) == r1
        assert get_xth(doc, 2) == r1
        assert get_xth(doc, 3) == r1
        assert get_xth(doc, 4) == r1
        assert str(doc[0]) == 'one'
        assert str(doc[1]) == 'two'
        assert str(doc[2]) == 'three'
        assert str(doc[3]) == 'four'
        assert str(doc[4]) == 'five'

    def test_workflow_09a(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'),
             if_str_token_back('four', -2),
             [m('cat'), m('puppy')],
             m('horse'),
             ]
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        r1 = 'mouse dog cat puppy horse'  # one, five
        # two, three
        r2 = 'mouse dog cat puppy horse mouse dog cat puppy horse'
        r3 = 'mouse dog mouse dog cat puppy horse'  # four
        assert get_xth(doc, 0) == r1
        assert get_xth(doc, 1) == r2
        assert get_xth(doc, 2) == r2
        assert get_xth(doc, 3) == r3
        assert get_xth(doc, 4) == r1
        assert str(doc[0]) == 'one'
        assert str(doc[1]) == 'two'
        assert str(doc[2]) == 'three'
        assert str(doc[3]) == 'four'
        assert str(doc[4]) == 'five'

    def test_workflow_09b(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'),
             if_str_token_forward('two', 2),
             [m('cat'), m('puppy')],
             m('horse'),
             ]
        ])
        doc = self.doc
        self.we.process(doc)
        t = get_first(doc)
        r1 = 'mouse dog cat puppy horse'  # one, four, five
        r2 = 'mouse dog'  # two
        r3 = None  # three
        assert get_xth(doc, 0) == r1
        assert get_xth(doc, 1) == r2
        assert get_xth(doc, 2) == r3
        assert get_xth(doc, 3) == r1
        assert get_xth(doc, 4) == r1
        assert str(doc[0]) == 'one'
        assert str(doc[1]) == 'two'
        assert str(doc[2]) == 'three'
        assert str(doc[3]) == 'four'
        assert str(doc[4]) == 'five'

    def test_workflow_21(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            if_str_token_forward('one', -1),
            m('horse'),
        ])
        doc = self.doc
        try:
            self.we.process(doc)
        except:
            t = get_first(doc)
            assert t == 'mouse'
        else:
            raise Exception("jumpTokenForward allowed negative number")

    def test_workflow_21b(self):
        self.we.addManyCallbacks(self.key, [
            m('mouse'),
            if_str_token_back('one', 1),
            m('horse'),
        ])
        doc = self.doc
        try:
            self.we.process(doc)
        except:
            t = get_first(doc)
            assert t == 'mouse'
        else:
            raise Exception("jumpTokenBack allowed positive number")

    # ----------------- HaltProcessing --------------------

    def test_workflow_30(self):

        doc = self.doc
        other_wfe = wfe_impl()
        wfe = self.we

        other_wfe.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'),
              halt_processing(),
              ],
             m('horse'),
             ]
        ])

        wfe.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'),
              lambda o, e: other_wfe.process(doc),
              ],
             m('horse'),
             ]
        ])
        try:
            wfe.process(doc)
        except HaltProcessing:
            pass
        except:
            raise

        t = get_first(doc)
        assert get_xth(
            doc, 0) == 'mouse dog cat puppy python mouse dog cat puppy python'
        assert get_xth(doc, 1) is None
        assert get_xth(doc, 2) is None
        # print wfe._i
        # print other_wfe._i

        assert str(doc[0]) == 'one'
        assert str(doc[1]) == 'two'
        assert str(doc[2]) == 'three'
        assert str(doc[3]) == 'four'
        assert str(doc[4]) == 'five'

    def test_workflow_31(self):
        '''Restart workflow'''

        doc = self.doc
        wfe = self.we

        wfe.addManyCallbacks(self.key, [
            m('mouse'),
            [m('dog'),
             [m('cat'), m('puppy')],
             [m('python'),
              halt_processing(),
              ],
             m('horse'),
             ]
        ])
        try:
            wfe.process(doc)
        except HaltProcessing:
            pass
        except:
            raise

        assert get_xth(doc, 0) == 'mouse dog cat puppy python'
        assert get_xth(doc, 1) is None
        assert get_xth(doc, 2) is None

        assert str(doc[0]) == 'one'
        assert str(doc[1]) == 'two'
        assert str(doc[2]) == 'three'
        assert str(doc[3]) == 'four'
        assert str(doc[4]) == 'five'

        # print wfe._i
        wfe._i[0] -= 1
        wfe._i[1][-1] += 1
        # this should pick up from the point where we stopped
        try:
            wfe.process(doc)
        except HaltProcessing:
            pass
        except:
            raise

        assert get_xth(doc, 0) == 'mouse dog cat puppy python horse'
        assert get_xth(doc, 1) == 'mouse dog cat puppy python'
        assert get_xth(doc, 2) is None

        assert str(doc[0]) == 'one'
        assert str(doc[1]) == 'two'
        assert str(doc[2]) == 'three'
        assert str(doc[3]) == 'four'
        assert str(doc[4]) == 'five'


def suite():
    suite = unittest.TestSuite()
    # suite.addTest(TestWorkflowEngine('test_workflow_30'))
    # suite.addTest(TestWorkflowEngine('test_workflow_01'))
    suite.addTest(unittest.makeSuite(TestWorkflowEngine))
    return suite

if __name__ == '__main__':
    # unittest.main()
    unittest.TextTestRunner(verbosity=2).run(suite())
