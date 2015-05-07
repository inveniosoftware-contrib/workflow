# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2012, 2014, 2015 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.


"""The workflow engine extension of GenericWorkflowEngine."""

from __future__ import absolute_import

import sys

import traceback
from collections import OrderedDict

from six import iteritems
from six.moves import cPickle
from .engine import (
    GenericWorkflowEngine,
    TransitionActions,
    ProcessingFactory,
    Break,
    Continue,
)
from .errors import AbortProcessing, SkipToken, WorkflowError
from .deprecation import deprecated
from .utils import staticproperty


class WorkflowStatus(object):
    """Define the known workflow statuses.

       ================  =============
       Attribute         Internal repr
       ================  =============
       NEW               0
       RUNNING           1
       HALTED            2
       ERROR             3
       COMPLETED         4
       ================  =============
    """

    NEW = 0
    RUNNING = 1
    HALTED = 2
    ERROR = 3
    COMPLETED = 4


# enum34 lib is not used here is because comparisons against non-enumeration
# values will always compare not equal (in other words, `0 in ObjectStatus`
# would always return `False`.
class _ObjectStatus(object):
    """Specify the known object statuses.

       ================ =================== =============
       Attribute        Human-friendly name Internal repr
       ================ =================== =============
       INTIAL           New                 0
       COMPLETED        Done                1
       HALTED           Need action         2
       RUNNING          In process          3
       WAITING          Waiting             4
       ERROR            Error               5
       ================ =================== =============
    """

    def __init__(self):
        self._statuses = OrderedDict((
            ('INITIAL', "New"),         # 0
            ('COMPLETED', "Done"),      # 1
            ('HALTED', "Need action"),  # 2
            ('RUNNING', "In process"),  # 3
            ('WAITING', "Waiting"),     # 4
            ('ERROR', "Error"),         # 5
        ))
        for idx, key in enumerate(self._statuses.keys()):
            setattr(self, key, idx)

    @property
    @deprecated("Please use ObjectStatus.COMPLETED "
                "instead of ObjectStatus.FINAL")
    def FINAL(self):
        """Return cls.COMPLETED, although this is deprecated."""
        return self.COMPLETED  # pylint: disable=no-member

    def name(self, version):
        """Human readable name from the integer state representation."""
        return self._statuses.values()[version]

    def __contains__(self, val):
        return val in range(len(self._statuses))

# Required in order to be able to implement `__contains__`.
ObjectStatus = _ObjectStatus()


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
        self.save()
        # To initialize the logger, `db_obj` must be first set. For this we
        # must have saved at least once before calling `__init__`.
        super(DbWorkflowEngine, self).__init__()

    @classmethod
    def with_name(cls, name, curr_obj=0, id_user=0, module_name="Unknown",
                  **kwargs):
        """ Instantiate a DbWorkflowEngine given a name or UUID.

        :param name: name of workflow to run.
        :type name: str

        :param curr_obj: internal id of current object being processed.
        :type curr_obj: int

        :param id_user: id of user to associate with workflow
        :type id_user: int

        :param module_name: label used to query groups of workflows.
        :type module_name: str
        """
        # FIXME: current_object is unused (?)
        db_obj = Workflow(
            name=name,
            id_user=id_user,
            current_object=curr_obj,
            module_name=module_name,
            uuid=new_uuid()
        )
        return cls(db_obj, **kwargs)

    @classmethod
    def from_uuid(cls, uuid, **kwargs):
        """ Load a workflow from the database given a UUID.

        :param uuid: pass a uuid to an existing workflow.
        :type uuid: str
        """
        db_obj = Workflow.get(Workflow.uuid == uuid).first()
        if db_obj is None:
            raise LookupError("No workflow with UUID {} was found".format(uuid))
        return cls(db_obj, **kwargs)

    @staticproperty
    def processing_factory():  # pylint: disable=no-method-argument
        """Provide a proccessing factory."""
        return DbProcessingFactory

    ############################################################################
    #                                                                          #
    # Deprecated
    @property
    def counter_object(self):
        """Return the number of object."""
        raise NotImplementedError

    @property
    def name(self):
        """Return the name."""
        return self.db_obj.name

    @property
    def status(self):
        """Return the status."""
        return self.db_obj.status

    # XXX renamed recently from 'objects'
    @property
    def database_objects(self):
        """Return the objects associated with this workflow."""
        return self.db_obj.objects

    @property
    def final_objects(self):
        """Return the objects associated with this workflow."""
        return [obj for obj in self.database_objects
                if obj.version in [ObjectStatus.COMPLETED]]  #  pylint: disable=no-member

    @property
    def halted_objects(self):
        """Return the objects associated with this workflow."""
        return [obj for obj in self.database_objects
                if obj.version in [ObjectStatus.HALTED]]  #  pylint: disable=no-member

    @property
    def running_objects(self):
        """Return the objects associated with this workflow."""
        return [obj for obj in self.database_objects
                if obj.version in [ObjectStatus.RUNNING]]  #  pylint: disable=no-member
    #                                                                          #
    ############################################################################

    def __getstate__(self):
        """Pickling needed functions."""
        if not self._picklable_safe:
            raise cPickle.PickleError("The instance of the workflow engine "
                                      "cannot be serialized, "
                                      "because it was constructed with "
                                      "custom, user-supplied callbacks. "
                                      "Either use PickableWorkflowEngine or "
                                      "provide your own __getstate__ method.")
        state = self.__dict__.copy()
        del state['log']
        return state

    def __setstate__(self, state):
        """Unpickling needed functions."""
        if len(self._objects) < self._i[0]:
            raise cPickle.PickleError("The workflow instance "
                                      "inconsistent state, "
                                      "too few objects")

        db_handler_obj = DbWorkflowLogHandler(DbWorkflowEngineLog, "uuid")
        state['log'] = get_logger(logger_name="workflow.%s" % state['uuid'],
                                  db_handler_obj=db_handler_obj,
                                  obj=self)

        self.__dict__ = state

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

    ############################################################################
    #                                                                          #

    # TODO: Modify the counters via signals instead
    def set_counter_initial(self, obj_count):
        """Initiate the counters of object states.

        :param obj_count: Number of objects to process.
        :type obj_count: int
        """
        self.db_obj.counter_initial = obj_count
        self.db_obj.counter_halted = 0
        self.db_obj.counter_error = 0
        self.db_obj.counter_finished = 0

    def increase_counter_halted(self):
        """Indicate we halted the processing of one object."""
        self.db_obj.counter_halted += 1

    def increase_counter_error(self):
        """Indicate we crashed the processing of one object."""
        self.db_obj.counter_error += 1

    def increase_counter_finished(self):
        """Indicate we finished the processing of one object."""
        self.db_obj.counter_finished += 1

    #                                                                          #
    ############################################################################

    def abortProcessing(self):
        """Abort current workflow execution without saving object."""
        raise AbortProcessing

    def skipToken(self):
        """Skip current workflow object without saving it."""
        raise SkipToken


class DbTransitionAction(TransitionActions):
    """Transition actions on engine exceptions for persistence object."""

    @staticmethod
    def SkipToken(obj, eng, callbacks, e):
        """Action to take when SkipToken is raised."""
        msg = "Skipped running this object: '%s' (object: %s)" % \
            (str(callbacks), repr(obj))
        eng.log.debug(msg)
        obj.log.debug(msg)
        raise Continue

    @staticmethod
    def AbortProcessing(obj, eng, callbacks, e):
        """Action to take when AbortProcessing is raised."""
        msg = "Processing was aborted: '%s' (object: %s)" % \
            (str(callbacks), repr(obj))
        eng.log.debug(msg)
        obj.log.debug(msg)
        raise Break

    @staticmethod
    def HaltProcessing(obj, eng, callbacks, exc_info):
        """Action to take when HaltProcessing is raised."""
        e = exc_info[1]
        eng.increase_counter_halted()
        if e.action:
            obj.set_action(e.action, e.message)
            version = ObjectStatus.HALTED  #  pylint: disable=no-member
        else:
            version = ObjectStatus.WAITING  #  pylint: disable=no-member
        obj.save(version=version, task_counter=eng.state.task_pos,
                 id_workflow=eng.uuid)
        eng.save(status=WorkflowStatus.HALTED)
        message = "Workflow '%s' halted at task %s with message: %s" % \
                    (eng.name, eng.current_taskname or "Unknown", e.message)
        eng.log.warning(message)
        super(DbTransitionAction, DbTransitionAction).HaltProcessing(obj, eng, callbacks, exc_info)

    @staticmethod
    def WorkflowError(obj, eng, callbacks, e):
        raise

    @staticmethod
    def Exception(obj, eng, callbacks, exc_info):
        """Action to take when an otherwise unhandled exception is raised."""
        exception_repr = ''.join(traceback.format_exception(*exc_info))
        msg = "Error:\n%s" % (exception_repr)
        eng.log.error(msg)
        eng.increase_counter_error()
        if obj:
            # Sets an error message as a tuple (title, details)
            obj.set_error_message(exception_repr)
            obj.save(ObjectStatus.ERROR, eng.state.task_pos,  #  pylint: disable=no-member
                     id_workflow=eng.uuid)
        eng.save(status=WorkflowStatus.ERROR)
        traceback.print_exception(*exc_info, file=sys.stderr)
        try:
            super(DbTransitionAction, DbTransitionAction).Exception(obj, eng, callbacks, exc_info)
        except Exception:
            # We expect this to reraise
            pass
        raise WorkflowError(
            message=exception_repr,
            id_workflow=eng.uuid,
            id_object=eng.state.elem_ptr,
        )


class DbProcessingFactory(ProcessingFactory):
    """Processing factory for persistence requirements."""

    @staticproperty
    def transition_exception_mapper():  # pylint: disable=no-method-argument
        """Define our for handling transition exceptions."""
        return DbTransitionAction

    @staticmethod
    def before_object(eng, objects, obj):
        """Action to take before the proccessing of an object begins."""
        obj.save(version=ObjectStatus.RUNNING, id_workflow=eng.db_obj.uuid)  #  pylint: disable=no-member
        super(DbProcessingFactory, DbProcessingFactory).before_object(eng, objects, obj)

    @staticmethod
    def after_object(eng, objects, obj):
        """Action to take once the proccessing of an object completes."""
        # We save each object once it is fully run through
        obj.save(version=ObjectStatus.COMPLETED)  #  pylint: disable=no-member
        eng.increase_counter_finished()
        super(DbProcessingFactory, DbProcessingFactory).after_object(eng, objects, obj)

    @staticmethod
    def before_processing(eng, objects):
        """Executed before processing the workflow."""
        eng.save(status=WorkflowStatus.RUNNING)
        eng.set_counter_initial(len(objects))
        super(DbProcessingFactory, DbProcessingFactory).before_processing(eng, objects)

    @staticmethod
    def after_processing(eng, objects):
        """Action after process to update status."""
        if eng.has_completed:
            eng.save(WorkflowStatus.COMPLETED)
        else:
            eng.save(WorkflowStatus.HALTED)
