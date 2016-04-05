# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2012, 2014, 2015, 2016 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""The workflow engine extension of GenericWorkflowEngine."""

from __future__ import absolute_import

import traceback

from enum import Enum

from six import reraise

from .engine import (
    GenericWorkflowEngine,
    TransitionActions,
    ProcessingFactory,
)
from .errors import WorkflowError
from .utils import classproperty


class EnumLabel(Enum):
    def __init__(self, label):
        self.label = self.labels[label]

    @classproperty
    def labels(cls):
        raise NotImplementedError


class WorkflowStatus(EnumLabel):
    """Define the known workflow statuses. """

    NEW = 0
    RUNNING = 1
    HALTED = 2
    ERROR = 3
    COMPLETED = 4

    @classproperty
    def labels(cls):
        return {
            0: "New",
            1: "Running",
            2: "Halted",
            3: "Error",
            4: "Completed",
        }


class ObjectStatus(EnumLabel):
    """Specify the known object statuses."""

    INITIAL = 0
    COMPLETED = 1
    HALTED = 2
    RUNNING = 3
    ERROR = 4

    @classproperty
    def labels(cls):
        return {
            0: "New",
            1: "Done",
            2: "Need action",
            3: "In process",
            4: "Error",
        }


class DbWorkflowEngine(GenericWorkflowEngine):
    """GenericWorkflowEngine with DB persistence.

    Adds a SQLAlchemy database model to save workflow states and
    workflow data.

    Overrides key functions in GenericWorkflowEngine to implement
    logging and certain workarounds for storing data before/after
    task calls (This part will be revisited in the future).
    """

    def __init__(self, db_obj, **kwargs):
        """Instantiate a new BibWorkflowEngine object.

        :param db_obj: the workflow engine
        :type db_obj: Workflow

        This object is needed to run a workflow and control the workflow,
        like at which step of the workflow execution is currently at, as well
        as control object manipulation inside the workflow.

        You can pass several parameters to personalize your engine,
        but most of the time you will not need to create this object yourself
        as the :py:mod:`.api` is there to do it for you.

        :param db_obj: instance of a Workflow object.
        :type db_obj: Workflow
        """
        self.db_obj = db_obj
        super(DbWorkflowEngine, self).__init__()

    @classproperty
    def processing_factory(cls):
        """Provide a proccessing factory."""
        return DbProcessingFactory

    @classproperty
    def known_statuses(cls):
        return WorkflowStatus

    @property
    def name(self):
        """Return the name."""
        return self.db_obj.name

    @property
    def status(self):
        """Return the status."""
        return self.db_obj.status

    @property
    def uuid(self):
        """Return the status."""
        return self.db_obj.uuid

    @property
    def database_objects(self):
        """Return the objects associated with this workflow."""
        return self.db_obj.objects

    @property
    def final_objects(self):
        """Return the objects associated with this workflow."""
        return [obj for obj in self.database_objects
                if obj.status in [obj.known_statuses.COMPLETED]]

    @property
    def halted_objects(self):
        """Return the objects associated with this workflow."""
        return [obj for obj in self.database_objects
                if obj.status in [obj.known_statuses.HALTED]]

    @property
    def running_objects(self):
        """Return the objects associated with this workflow."""
        return [obj for obj in self.database_objects
                if obj.status in [obj.known_statuses.RUNNING]]

    def __repr__(self):
        """Allow to represent the DbWorkflowEngine."""
        return "<DbWorkflow_engine(%s)>" % (self.name,)

    def __str__(self, log=False):
        """Allow to print the DbWorkflowEngine."""
        return """-------------------------------
DbWorkflowEngine
-------------------------------
    %s
-------------------------------
""" % (self.db_obj.__str__(),)

    def save(self, status=None):
        """Save the workflow instance to database."""
        # This workflow continues a previous execution.
        self.db_obj.save(status)


class DbTransitionAction(TransitionActions):
    """Transition actions on engine exceptions for persistence object.

    ..note::
        Typical actions to take here is store the new state of the object and
        save it, save the engine, log a message and finally call `super`.
    """
    @staticmethod
    def HaltProcessing(obj, eng, callbacks, exc_info):
        """Action to take when HaltProcessing is raised."""
        e = exc_info[1]
        obj.save(status=obj.known_statuses.HALTED,
                 task_counter=eng.state.callback_pos,
                 id_workflow=eng.uuid)
        eng.save(status=WorkflowStatus.HALTED)
        message = "Workflow '%s' halted at task %s with message: %s" % \
                  (eng.name, eng.current_taskname or "Unknown", e.message)
        eng.log.warning(message)
        super(DbTransitionAction, DbTransitionAction).HaltProcessing(
            obj, eng, callbacks, exc_info
        )

    @staticmethod
    def Exception(obj, eng, callbacks, exc_info):
        """Action to take when an otherwise unhandled exception is raised."""
        exception_repr = ''.join(traceback.format_exception(*exc_info))
        msg = "Error:\n%s" % (exception_repr)
        eng.log.error(msg)
        if obj:
            # Sets an error message as a tuple (title, details)
            obj.set_error_message(exception_repr)
            obj.save(status=obj.known_statuses.ERROR,
                     callback_pos=eng.state.callback_pos,
                     id_workflow=eng.uuid)
        eng.save(WorkflowStatus.ERROR)
        try:
            super(DbTransitionAction, DbTransitionAction).Exception(
                obj, eng, callbacks, exc_info
            )
        except Exception:
            # We expect this to reraise
            pass
        # Change the type of the Exception to WorkflowError, but use its tb
        reraise(WorkflowError(
            message=exception_repr, id_workflow=eng.uuid,
            id_object=eng.state.token_pos), None, exc_info[2]
        )


class DbProcessingFactory(ProcessingFactory):
    """Processing factory for persistence requirements."""

    @classproperty
    def transition_exception_mapper(cls):
        """Define our for handling transition exceptions."""
        return DbTransitionAction

    @staticmethod
    def before_object(eng, objects, obj):
        """Action to take before the proccessing of an object begins."""
        obj.save(status=obj.known_statuses.RUNNING,
                 id_workflow=eng.db_obj.uuid)
        super(DbProcessingFactory, DbProcessingFactory).before_object(
            eng, objects, obj
        )

    @staticmethod
    def after_object(eng, objects, obj):
        """Action to take once the proccessing of an object completes."""
        # We save each object once it is fully run through
        obj.save(status=obj.known_statuses.COMPLETED,
                 id_workflow=eng.db_obj.uuid)
        super(DbProcessingFactory, DbProcessingFactory).after_object(
            eng, objects, obj
        )

    @staticmethod
    def before_processing(eng, objects):
        """Executed before processing the workflow."""
        eng.save(WorkflowStatus.RUNNING)
        super(DbProcessingFactory, DbProcessingFactory).before_processing(
            eng, objects
        )

    @staticmethod
    def after_processing(eng, objects):
        """Action after process to update status."""
        if eng.has_completed:
            eng.save(WorkflowStatus.COMPLETED)
        else:
            eng.save(WorkflowStatus.HALTED)
