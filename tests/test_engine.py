# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2014, 2015, 2016 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

import os
import sys
import mock

import pytest

import re

from workflow.patterns.controlflow import IF_ELSE
from workflow.engine import GenericWorkflowEngine, HaltProcessing
from workflow.errors import WorkflowError


p = os.path.abspath(os.path.dirname(__file__) + '/../')
if p not in sys.path:
    sys.path.append(p)


def m(key=None):
    def _m(token, inst):
        current_sem = token.getFeature('sem', '')
        new_feature = (current_sem + ' ' + key).strip()
        token.setFeatureKw(sem=new_feature)
    _m.__name__ = 'string appender'
    return _m


def if_str_token_jump(value='', step=0):
    def x(token, inst):
        if step >= 0:
            feature_str = 'token_back'
        else:
            feature_str = 'token_forward'
        if str(token) == value and not token.getFeature(feature_str):
            token.setFeature(feature_str, 1)
            inst.jump_token(step)
    return lambda token, inst: x(token, inst)


def jump_call(step=0):
    if step < 0:
        def x(token, inst):
            if not token.getFeature('back'):
                token.setFeature('back', 1)
                inst.jump_call(step)
        return lambda token, inst: x(token, inst)
    return lambda token, inst: inst.jump_call(step)


def break_loop():
    return lambda token, inst: inst.break_current_loop()


def workflow_error():
    def _error(token, inst):
        raise WorkflowError("oh no!")
    return _error


def stop_processing():
    return lambda token, inst: inst.stop()


def halt_processing():
    return lambda token, inst: inst.halt()


def next_token():
    return lambda token, inst: inst.continue_next_token()


def get_first(doc):
    return doc[0].getFeature('sem')


def get_xth(doc, xth):
    return doc[xth].getFeature('sem')


def stop_if_token_equals(value=None):
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

    def getFeature(self, key, default=None):
        try:
            return self.__attributes[key]
        except KeyError:
            return default

    def setFeature(self, key, value):
        self.__attributes[key] = value

    def setFeatureKw(self, **kwargs):
        for key, value in kwargs.items():
            self.setFeature(key, value)


class TestWorkflowEngine(object):

    """Tests using FakeTokens in place of strings"""

    def setup_method(self, method):
        self.key = '*'
        self.wfe = GenericWorkflowEngine()
        self.data = ['one', 'two', 'three', 'four', 'five']
        self.tokens = [FakeToken(x, type='*') for x in self.data]

    def teardown_method(self, method):
        pass

    @pytest.mark.parametrize("_,tokens,exception,exception_msg", (
        ("int", 49, WorkflowError, "not an iterable"),
        ("str", "hello", WorkflowError, "not an iterable"),
        ("object", object, WorkflowError, "not an iterable"),
    ))
    def test_objects_are_of_bad_type(self, _, tokens, exception, exception_msg):
        with pytest.raises(exception) as exc_info:
            self.wfe.process(tokens)
        assert exception_msg in exc_info.value.args[0]

    def test_empty_object_list_logs_warning(self):
        assert hasattr(self.wfe, 'log')
        self.wfe.log = mock.Mock()
        self.wfe.callbacks.replace([lambda o, e: None])
        self.wfe.process([])
        self.wfe.log.warning.assert_called_once_with('List of objects is empty. Running workflow '
                                                     'on empty set has no effect.')

    def test_current_taskname_resolution(self):
        workflow = [m('test')]
        self.wfe.callbacks.replace(workflow, self.key)
        self.wfe.process(self.tokens)
        assert self.wfe.current_taskname == 'string appender'

        workflow = [lambda obj, eng: 1]
        self.wfe.callbacks.replace(workflow, self.key)
        self.wfe.process(self.tokens)
        assert self.wfe.current_taskname == '<lambda>'

        workflow = [
            IF_ELSE(
                lambda obj, eng: True,
                [lambda obj, eng: 1],
                [lambda obj, eng: 2],
            )
        ]
        self.wfe.callbacks.replace(workflow, self.key)
        # This test will break if someone changes IF_ELSE. TODO: Mock
        # Note: Python3 has much stronger introspection, thus the `.*`.
        assert re.match(r'\[<function IF_ELSE.* at 0x[0-f]+>, '
                        r'\[<function .*<lambda> at 0x[0-f]+>\], '
                        r'<function BREAK.* at 0x[0-f]+>, '
                        r'\[<function .*<lambda> at 0x[0-f]+>\]\]',
                        self.wfe.current_taskname)

    def test_current_object_returns_correct_object(self):
        self.wfe.callbacks.replace([halt_processing()])

        assert self.wfe.current_object is None
        with pytest.raises(HaltProcessing):
            self.wfe.process(self.tokens)
        assert self.wfe.current_object is self.tokens[0]
        with pytest.raises(HaltProcessing):
            self.wfe.restart('current', 'next')
        assert self.wfe.current_object is self.tokens[1]

    @pytest.mark.parametrize("_,callbacks,expected_result", (
        (
            'skips_forward_with_acceptable_increment',
            [
                m('mouse'),
                [m('dog'), jump_call(2), m('cat'), m('puppy'), m('python')],
                m('horse'),
            ],
            'mouse dog puppy python horse'
        ),

        (
            'skips_forward_with_increment_that_is_too_large',
            [
                m('mouse'),
                [m('dog'), jump_call(50), m('cat'), m('puppy'), m('python')],
                m('horse'),
            ],
            'mouse dog horse'
        ),

        (
            'jumps_forward_outside_of_nest',
            [
                jump_call(3),
                m('mouse'),
                [m('dog'), m('cat'), m('puppy'), m('python')],
                m('horse'),
            ],
            'horse'
        ),

        (
            'skips_backwards_with_acceptable_decrement',
            [
                m('mouse'),
                [m('dog'), jump_call(-1), m('cat'), m('puppy')],
                m('horse'),
            ],
            'mouse dog dog cat puppy horse'
        ),

        (
            'skips_backwards_with_decrement_that_is_too_large',
            [
                m('mouse'),
                [m('dog'), m('cat'), jump_call(-50), m('puppy'), m('python')],
                m('horse'),
            ],
            'mouse dog cat dog cat puppy python horse'
        ),

        (
            'skips_backwards_outside_of_nest',
            [
                m('mouse'),
                [m('dog'), m('cat'), m('puppy'), m('python')],
                m('horse'),
                jump_call(-2)
            ],
            'mouse dog cat puppy python horse dog cat puppy python horse'
        )
    ))
    def test_jump_call(self, _, callbacks, expected_result):
        self.wfe.callbacks.add_many(callbacks, self.key)
        self.wfe.process(self.tokens)
        t = get_first(self.tokens)
        assert t == expected_result

    # --------- complicated loop -----------

    @pytest.mark.parametrize("_,workflow,expected_result", (
        (
            'simple',
            [
                m('mouse'),
                [
                    m('dog'),
                    [m('cat'), m('puppy')],
                    [m('python'), [m('wasp'), m('leon')]],
                    m('horse'),
                ]
            ],
            'mouse dog cat puppy python wasp leon horse'
        ),

        (
            'with_nested_jumps',
            [
                jump_call(2),
                m('mouse'),
                [
                    m('dog'),
                    [m('cat'), m('puppy')],
                    [m('python'), jump_call(-2), [m('wasp'), m('leon')]],
                    m('horse'),
                ]
            ],
            'dog cat puppy python python wasp leon horse'
        )
    ))
    def test_multi_nested_workflows(self, _, workflow, expected_result):
        self.wfe.callbacks.add_many(workflow, self.key)
        self.wfe.process(self.tokens)
        t = get_first(self.tokens)
        assert t == expected_result

    @pytest.mark.parametrize("_,workflow,expected_result", (
        (
            'simple',
            [
                m('mouse'),
                [
                    m('dog'),
                    [m('cat'), m('puppy')],
                    [m('python'), break_loop(), [m('wasp'), m('leon')]],
                    m('horse'),
                ]
            ],
            'mouse dog cat puppy python horse'
        ),

        (
            'break_loop_outside_of_nest',
            [
                break_loop(),
                m('mouse'),
                [
                    m('dog'),
                    [m('cat'), m('puppy')],
                    [m('python'), [m('wasp'), m('leon')]],
                    m('horse'),
                ]
            ],
            None
        )
    ))
    def test_break_from_this_loop(self, _, workflow, expected_result):
        self.wfe.callbacks.add_many(workflow, self.key)
        self.wfe.process(self.tokens)
        t = get_first(self.tokens)
        assert t == expected_result

    # ----------- StopProcessing --------------------------

    def test_engine_immediatelly_stops(self):
        self.wfe.callbacks.add_many([
            stop_processing(),
            m('mouse'),
            [
                m('dog'),
                [m('cat'), m('puppy')],
                [
                    m('python'),
                    [m('wasp'), m('leon')]
                ],
                m('horse'),
            ]
        ], self.key)
        self.wfe.process(self.tokens)
        t = get_first(self.tokens)
        assert get_xth(self.tokens, 0) is None
        assert get_xth(self.tokens, 1) is None
        assert get_xth(self.tokens, 2) is None

    def test_engine_stops_half_way_through(self):
        self.wfe.callbacks.add_many([
            m('mouse'),
            [
                m('dog'),
                [m('cat'), m('puppy')],
                [m('python'), stop_if_token_equals('four'), [m('wasp'), m('leon')]],
                m('horse'),
            ]
        ], self.key)
        self.wfe.process(self.tokens)
        full_result = 'mouse dog cat puppy python wasp leon horse'
        result_until_stop = 'mouse dog cat puppy python'
        assert get_xth(self.tokens, 0) == full_result          # 'one'
        assert get_xth(self.tokens, 1) == full_result          # 'two'
        assert get_xth(self.tokens, 2) == full_result          # 'three'
        assert get_xth(self.tokens, 3) == result_until_stop    # 'four'
        assert get_xth(self.tokens, 4) is None  # 'five', engine stopped

    # ---------- jump_token -------------

    def test_engine_moves_to_next_token(self):
        self.wfe.callbacks.add_many(
            [
                m('mouse'),
                [
                    m('dog'),
                    [m('cat'), m('puppy')],
                    [m('python'), next_token(), [m('wasp'), m('leon')]],
                    m('horse'),
                ]
            ], self.key)
        self.wfe.process(self.tokens)
        result_until_next_token = 'mouse dog cat puppy python'
        for i in range(5):
            assert get_xth(self.tokens, i) == result_until_next_token

    def test_workflow_09a(self):
        self.wfe.callbacks.add_many([
            m('mouse'),
            [
                m('dog'), if_str_token_jump('four', -2),
                [m('cat'), m('puppy')],
                m('horse'),
            ]
        ], self.key)
        self.wfe.process(self.tokens)
        t = get_first(self.tokens)
        r1 = 'mouse dog cat puppy horse'  # one, five
        r2 = 'mouse dog cat puppy horse mouse dog cat puppy horse'  # two, three
        r3 = 'mouse dog mouse dog cat puppy horse'  # four
        assert get_xth(self.tokens, 0) == r1
        assert get_xth(self.tokens, 1) == r2
        assert get_xth(self.tokens, 2) == r2
        assert get_xth(self.tokens, 3) == r3
        assert get_xth(self.tokens, 4) == r1

    def test_workflow_09b(self):
        self.wfe.callbacks.add_many([
            m('mouse'),
            [
                m('dog'),
                if_str_token_jump('two', 2),
                [m('cat'), m('puppy')],
                m('horse'),
            ]
        ], self.key)
        self.wfe.process(self.tokens)
        t = get_first(self.tokens)
        r1 = 'mouse dog cat puppy horse'  # one, four, five
        r2 = 'mouse dog'  # two
        r3 = None  # three
        assert get_xth(self.tokens, 0) == r1
        assert get_xth(self.tokens, 1) == r2
        assert get_xth(self.tokens, 2) == r3
        assert get_xth(self.tokens, 3) == r1
        assert get_xth(self.tokens, 4) == r1

    # ----------------- HaltProcessing --------------------

    def test_50_halt_processing_mid_workflow(self):
        other_wfe = GenericWorkflowEngine()
        other_wfe.callbacks.add_many([
            m('mouse'),
            [
                m('dog'),
                [m('cat'), m('puppy')],
                [m('python'), halt_processing()],
                m('horse'),
            ]
        ], self.key)
        with pytest.raises(HaltProcessing):
            other_wfe.process(self.tokens)

        t = get_first(self.tokens)
        assert get_xth(self.tokens, 0) == 'mouse dog cat puppy python'
        assert get_xth(self.tokens, 1) is None
        assert get_xth(self.tokens, 2) is None

    compl = 'mouse dog cat puppy python horse'
    compl1 = 'mouse dog cat puppy python'

    @pytest.mark.parametrize("obj,task,results", (
        ('prev', 'prev', (compl + " python", compl1, None)),
        ('prev', 'current', (compl, compl1, None)),  # current task is to halt
        ('prev', 'next', (compl + " horse", compl1 +  " " + compl1, None)),
        ('prev', 'first', (compl + " " + compl1, compl1, None)),

        ('current', 'prev', (compl, compl1 + " python", None)),
        ('current', 'current', (compl, compl1, None)),
        ('current', 'next', (compl, compl1 + " horse", compl1)),
        ('current', 'first', (compl, compl1 + " " + compl1, None)),

        ('next', 'prev', (compl, compl1, "python")),
        ('next', 'current', (compl, compl1, None)),
        ('next', 'next', (compl, compl1, "horse")),
        ('next', 'first', (compl, compl1, compl1)),

        ('first', 'prev', (compl + " python", compl1, None)),
        ('first', 'current', (compl, compl1, None)),  # current task is to halt
        ('first', 'next', (compl + " horse", compl1 +  " " + compl1, None)),
        ('first', 'first', (compl + " " + compl1, compl1, None)),
    ))
    def test_51_workflow_restart_after_halt(self, obj, task, results):
        self.wfe.callbacks.add_many([
            m('mouse'),
            [
                m('dog'),
                [m('cat'), m('puppy')],
                [m('python'), halt_processing()],
                m('horse'),
            ]
        ], self.key)
        with pytest.raises(HaltProcessing):
            self.wfe.process(self.tokens)

        assert get_xth(self.tokens, 0) == 'mouse dog cat puppy python'
        assert get_xth(self.tokens, 1) is None
        assert get_xth(self.tokens, 2) is None

        # this should pick up from the point where we stopped
        with pytest.raises(HaltProcessing):
            self.wfe.restart('current', 'next')

        assert get_xth(self.tokens, 0) == 'mouse dog cat puppy python horse'
        assert get_xth(self.tokens, 1) == 'mouse dog cat puppy python'
        assert get_xth(self.tokens, 2) is None

        with pytest.raises(HaltProcessing):
            self.wfe.restart(obj, task)

        assert (get_xth(self.tokens, 0),
                get_xth(self.tokens, 1),
                get_xth(self.tokens, 2)) == results

    def test_restart_accepts_new_objects(self):
        workflow = [m('test')]
        self.wfe.callbacks.replace(workflow, self.key)
        self.wfe.process(self.tokens)

        new_data = ['a', 'b', 'c', 'd', 'e']
        new_tokens = [FakeToken(x, type='*') for x in new_data]

        self.wfe.restart('first', 'first', objects=new_tokens)

        assert self.wfe.objects == new_tokens

    def test_has_completed(self):
        self.wfe.callbacks.replace([
            m('mouse'),
            halt_processing(),
            m('horse'),
        ])
        assert self.wfe.has_completed is False
        with pytest.raises(HaltProcessing):
            self.wfe.process([self.tokens[0]])
        assert self.wfe.has_completed is False
        self.wfe.restart('current', 'next')
        assert self.wfe.has_completed is True

    def test_nested_workflow_halt(self):
        other_wfe = GenericWorkflowEngine()
        wfe = self.wfe

        other_wfe.callbacks.add_many([
            m('mouse'),
            [
                m('dog'),
                [m('cat'), m('puppy')],
                [m('python'), halt_processing()],
                m('horse'),
            ]
        ], self.key)

        wfe.callbacks.add_many([
            m('mouse'),
            [
                m('dog'),
                [m('cat'), m('puppy')],
                [m('python'), lambda o, e: other_wfe.process(self.tokens)],
                m('horse'),
            ]
        ], self.key)
        with pytest.raises(HaltProcessing):
            wfe.process(self.tokens)

        t = get_first(self.tokens)
        assert get_xth(self.tokens, 0) == 'mouse dog cat puppy python mouse dog cat puppy python'
        assert get_xth(self.tokens, 1) is None
        assert get_xth(self.tokens, 2) is None

    @pytest.mark.parametrize("callbacks,kwargs,result,exception", (
        (
            [
                m('mouse'),
                [
                    m('dog'),
                    [m('cat'), m('puppy')],
                    [m('python'), workflow_error()],
                    m('horse'),
                ]
            ],
            {},
            'mouse dog cat puppy python',
            WorkflowError,
        ),
        (
            [
                m('mouse'),
                [
                    m('dog'),
                    [m('cat'), m('puppy')],
                    [m('python'), workflow_error()],
                    m('horse'),
                ]
            ],
            {
                'stop_on_error': False,
            },
            'mouse dog cat puppy python',
            None,
        ),
        (
            [
                m('mouse'),
                [
                    m('dog'),
                    [m('cat'), m('puppy')],
                    [m('python'), halt_processing()],
                    m('horse'),
                ]
            ],
            {
                'stop_on_halt': False,
            },
            'mouse dog cat puppy python',
            None,
        ),
    ))
    def test_process_smash_through(self, callbacks, kwargs, result, exception):
        self.wfe.callbacks.add_many(callbacks, self.key)

        if exception:
            with pytest.raises(exception):
                self.wfe.process(self.tokens, **kwargs)
        else:
            self.wfe.process(self.tokens, **kwargs)
            for idx, dummy in enumerate(self.tokens):
                assert get_xth(self.tokens, idx) == result
