# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2014, 2015 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

import os
import sys
from copy import deepcopy

import mock
import pytest
from six import iteritems

from workflow.engine import MachineState, Callbacks, GenericWorkflowEngine


p = os.path.abspath(os.path.dirname(__file__) + '/../')
if p not in sys.path:
    sys.path.append(p)


def obj_append(key):
    def _m(obj, eng):
        obj.append(key)
    return _m


def stop_if_obj_str_eq(value):
    def x(obj, eng):
        if str(obj) == value:
            eng.stopProcessing()
    return lambda obj, eng: x(obj, eng)


def jump_call(step=0):
    return lambda obj, eng: eng.jump_call(step)


class TestSignals(object):

    @pytest.mark.parametrize("signal_name", (
        'workflow_started',
        'workflow_halted',
        'workflow_finished',
    ))
    @pytest.mark.skipif(sys.version_info > (3, ),
                        reason="create_autospec broken in py3")
    def test_signals_are_emitted(self, signal_name):
        from workflow.engine import Signal
        from workflow import signals

        # Create engine
        eng = mock.create_autospec(GenericWorkflowEngine)

        # Call signal
        with mock.patch.object(signals, signal_name, autospec=True):
            getattr(Signal, signal_name)(eng)
            getattr(signals, signal_name).send.assert_called_once_with()

    def test_log_warning_if_signals_lib_is_missing(self):
        from workflow.engine import Signal

        orig_import = __import__

        def import_mock(name, *args):
            if name == 'workflow.signals':
                raise ImportError
            return orig_import(name, *args)

        # Patch the engine so that we can inspect calls
        with mock.patch('workflow.engine.GenericWorkflowEngine') as patched_GWE:
            eng = patched_GWE.return_value
        # Patch __import__ so that importing workflow.signals raises ImportError
        if sys.version_info < (3, ):
            builtins_module = '__builtin__'
        else:
            builtins_module = 'builtins'
        with mock.patch(builtins_module + '.__import__', side_effect=import_mock):
            Signal.workflow_started(eng)
        eng.log.warning.assert_called_once_with("Could not import signals lib; "
                                                "ignoring all future signal calls.")


def TestMachineState(object):

    def test_machine_state_does_not_allow_token_pos_below_minus_one(self):
        ms = MachineState()
        ms.token_pos = 1
        ms.token_pos = 0
        ms.token_pos = -1
        with pytest.raises(AttributeError):
            ms.token_pos = -2

    @pytest.mark.parametrize("params, token_pos, callback_pos", (
        (tuple(),       -1,     [0]),
        ((5, [1, 2]),   5,      [1, 2]),
    ))
    def test_machine_state_reads_defaults(self, params, token_pos, callback_pos):
        "Test initialization of machine state with and without args."""
        ms = MachineState(*params)
        assert ms.token_pos == token_pos
        assert ms.callback_pos == callback_pos

lmb = [
    lambda a: a,
    lambda b: b + 1,
    lambda c: c + 2,
    lambda d: d + 3
]


class TestCallbacks(object):

    @pytest.mark.parametrize("key,ret,exception", (
        ('*', [], KeyError),
        (None, {}, None),
    ))
    def test_callbacks_return_correct_when_empty(self, key, ret, exception):
        cbs = Callbacks()
        if exception:
            with pytest.raises(exception) as exc_info:
                cbs.get(key)
            assert 'No workflow is registered for the key: ' + key in exc_info.value.args[0]
        else:
            assert cbs.get(key) == ret

    @pytest.fixture()
    def cbs(self):
        return Callbacks()

    @pytest.mark.parametrize("in_dict,ret", (
        (
            {'a': [lmb[0], lmb[1]], 'b': [lmb[2], lmb[3]]},
            {'a': [lmb[0], lmb[1]], 'b': [lmb[2], lmb[3]]},
        ),
        (
            {'a': [lmb[0], (lmb[1], lmb[2])]},
            {'a': [lmb[0], lmb[1], lmb[2]]},
        ),
        (
            {'a': [lmb[0], ((lmb[1],), lmb[2])]},
            {'a': [lmb[0], lmb[1], lmb[2]]},
        ),
    ))
    def test_callbacks_get_return_correct_after_add_many(self, cbs, in_dict, ret):
        # Run `add_many`
        for key, val in iteritems(in_dict):
            cbs.add_many(val, key)
        # Existing keys
        for key, val in iteritems(ret):
            assert cbs.get(key) == val

    def test_callbacks_replace_from_used(self, cbs):
        cbs.add_many(lmb, '*')
        lmb_rev = lmb[::-1]
        cbs.replace(lmb_rev, '*')

        assert cbs.get('*') == lmb_rev

    def test_callbacks_clear_maintains_exception(self, cbs):
        cbs.add_many(lmb, 'some-key')
        cbs.clear()
        cbs.add_many(lmb, 'some-key')
        with pytest.raises(KeyError) as exc_info:
            cbs.get('missing')
        assert 'No workflow is registered for the key: ' + 'missing' in exc_info.value.args[0]


class TestGenericWorkflowEngine(object):

    """Tests of the WE interface"""

    def setup_method(self, method):
        # Don't turn this into some generator. One needs to be able to see what
        # the input is.
        self.d0 = [['one'], ['two'], ['three'], ['four'], ['five']]
        self.d1 = [['one'], ['two'], ['three'], ['four'], ['five']]
        self.d2 = [['one'], ['two'], ['three'], ['four'], ['five']]

    def teardown_method(self, method):
        pass

    def test_init(self):

        # init with empty to full parameters
        we1 = GenericWorkflowEngine()

        callbacks = [
            obj_append('mouse'),
            [obj_append('dog'), jump_call(1), obj_append('cat'), obj_append('puppy')],
            obj_append('horse'),
        ]

        we1.addManyCallbacks('*', deepcopy(callbacks))

        we1.process(self.d1)

    def test_configure(self):

        callbacks_list = [
            obj_append('mouse'),
            [obj_append('dog'), jump_call(1), obj_append('cat'), obj_append('puppy')],
            obj_append('horse'),
        ]

        we = GenericWorkflowEngine()
        we.addManyCallbacks('*', callbacks_list)

        # process using defaults
        we.process(self.d1)
        r = 'one mouse dog cat puppy horse'.split()

        we = GenericWorkflowEngine()
        we.addManyCallbacks('*', callbacks_list)
        we.process(self.d2)

        assert self.d1[0] == r
        assert self.d2[0] == r
        assert self.d1 == self.d2

    # ------------ tests configuring the we --------------------
    def test_workflow01(self):

        class GenericWEWithXChooser(GenericWorkflowEngine):
            def callback_chooser(self, obj):
                return self.callbacks.get('x')

        we0 = GenericWorkflowEngine()
        we1 = GenericWorkflowEngine()
        we2 = GenericWEWithXChooser()

        we0.addManyCallbacks('*', [
            obj_append('mouse'),
            [obj_append('dog'), jump_call(1), obj_append('cat'), obj_append('puppy')],
            obj_append('horse'),
        ])
        we1.setWorkflow([
            obj_append('mouse'),
            [obj_append('dog'), jump_call(1), obj_append('cat'), obj_append('puppy')],
            obj_append('horse'),
        ])
        we2.addManyCallbacks('x', [
            obj_append('mouse'),
            [obj_append('dog'), jump_call(1), obj_append('cat'), obj_append('puppy')],
            obj_append('horse'),
        ])

        we0.process(self.d0)
        we1.process(self.d1)
        we2.process(self.d2)

        assert self.d0 == self.d1
        assert self.d0 == self.d2
