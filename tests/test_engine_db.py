# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2015, 2016 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

import os
import sys

from collections import Iterable

import mock
import pytest

from workflow.engine import HaltProcessing, TransitionActions
from workflow.engine_db import (
    DbWorkflowEngine,
    ObjectStatus,
    WorkflowStatus,
    DbProcessingFactory,
)
from workflow.utils import classproperty


p = os.path.abspath(os.path.dirname(__file__) + '/../')
if p not in sys.path:
    sys.path.append(p)


class DummyDbObj(object):
    def __init__(self):
        self._status = None

    def save(self, status):
        pass

    @property
    def uuid(self):
        pass

    @property
    def name(self):
        pass

    @property
    def status(self):
        return self._status

    @property
    def objects(self):
        pass


def m(key=None):
    def _m(token, inst):
        token.data(key)
    _m.__name__ = 'string appender'
    return _m


class FakeToken(object):

    def __init__(self, data, **attributes):
        self.data = data
        self._status = None
        self._id_workflow = None  # TODO: Remove this
        self._callback_pos = None

    def save(self, status=None, callback_pos=None, id_workflow=None):
        self._status = status
        self._callback_pos = callback_pos
        self._id_workflow = id_workflow

    @classproperty
    def known_statuses(cls):
        return ObjectStatus


class TestObjectStatus(object):

    @pytest.mark.parametrize("status, name", (
        (ObjectStatus.INITIAL, "New"),
        (ObjectStatus.COMPLETED, "Done"),
        (ObjectStatus.HALTED, "Need action"),
        (ObjectStatus.RUNNING, "In process"),
        (ObjectStatus.ERROR, "Error"),
    ))
    def test_object_status_name_returns_correct_name(self, status, name):
        assert status.label == name


class TestWorkflowEngineDb(object):

    def setup_method(self, method):
        self.dummy_db_obj = mock.Mock(spec=DummyDbObj())
        self.dummy_db_obj.save(WorkflowStatus.NEW)
        self.wfe = DbWorkflowEngine(self.dummy_db_obj)
        self.data = ['one', 'two', 'three', 'four', 'five']
        self.tokens = [mock.Mock(spec=FakeToken(x)) for x in self.data]

    def teardown_method(self, method):
        pass

    @mock.patch.object(TransitionActions, 'HaltProcessing')
    def test_halt_processing_calls_parent(self, mock_HaltProcessing):
        self.wfe.callbacks.add_many([
            m('mouse'),
            lambda obj, eng: eng.halt()
        ])
        self.wfe.process(self.tokens)

        assert mock_HaltProcessing.call_count == len(self.data)
        for args_list in mock_HaltProcessing.call_args_list:
            args_list = args_list[0]
            assert isinstance(args_list[0], FakeToken)
            assert isinstance(args_list[1], DbWorkflowEngine)
            assert isinstance(args_list[2], Iterable)
            assert isinstance(args_list[3][1], HaltProcessing)  # exc_info

    def test_halt_processing_saves_eng_and_obj(self):
        self.wfe.callbacks.add_many([
            lambda obj, eng: eng.halt('please wait')
        ])
        with pytest.raises(HaltProcessing):
            self.wfe.process(self.tokens)

        token = self.tokens[0]

        assert token.save.call_count == 2
        assert token.save.call_args_list[0][1]['status'] == token.known_statuses.RUNNING
        assert token.save.call_args_list[1][1]['status'] == token.known_statuses.HALTED

    def test_halt_processing_saves_correct_statuses(self):
        self.wfe.callbacks.add_many([
            lambda obj, eng: eng.halt('please wait')
        ])
        with pytest.raises(HaltProcessing):
            self.wfe.process(self.tokens)

        # Token saved
        token = self.tokens[0]
        assert token.save.call_count == 2
        assert token.save.call_args_list[0][1]['status'] == token.known_statuses.RUNNING
        assert token.save.call_args_list[1][1]['status'] == token.known_statuses.HALTED

        # Engine saved
        assert self.dummy_db_obj.save.call_count == 3
        assert self.dummy_db_obj.save.call_args_list[0][0] == (WorkflowStatus.NEW, )
        assert self.dummy_db_obj.save.call_args_list[1][0] == (WorkflowStatus.RUNNING, )
        assert self.dummy_db_obj.save.call_args_list[2][0] == (WorkflowStatus.HALTED, )

    # Sorry, no parametrization here because mocks won't do.
    def test_before_object_save_object(self):
        DbProcessingFactory.before_object(self.wfe, self.tokens, self.tokens[0])
        assert self.tokens[0].save.call_count == 1
        assert self.tokens[0].save.call_args_list[0][1]['status'] == self.tokens[0].known_statuses.RUNNING

    # Sorry, no parametrization here because mocks won't do.
    def test_after_object_save_object(self):
        DbProcessingFactory.after_object(self.wfe, self.tokens, self.tokens[0])
        assert self.tokens[0].save.call_count == 1
        assert self.tokens[0].save.call_args_list[0][1]['status'] == self.tokens[0].known_statuses.COMPLETED

    @pytest.mark.parametrize("method, status, has_completed", (
        (DbProcessingFactory.before_processing, WorkflowStatus.RUNNING, False),
        (DbProcessingFactory.after_processing, WorkflowStatus.HALTED, False),
        (DbProcessingFactory.after_processing, WorkflowStatus.COMPLETED, True),
    ))
    def test_after_processing_save_status(self, method, status, has_completed):
        self.wfe.__class__.has_completed = mock.PropertyMock(return_value=has_completed)
        with mock.patch.object(self.wfe, 'save'):
            method(self.wfe, self.tokens)
            assert self.wfe.save.call_count == 1
            assert self.wfe.save.call_args_list[0][0] == (status, )

    def test_before_processing_save_status(self):
        with mock.patch.object(self.wfe, 'save'):
            self.wfe.processing_factory.before_processing(self.wfe, [])
            assert self.wfe.save.call_count == 1
            assert self.wfe.save.call_args_list[0][0] == (WorkflowStatus.RUNNING,)
