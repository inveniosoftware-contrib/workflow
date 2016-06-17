=============================
 Workflow v2.0.0 is released
=============================

Workflow v2.0.0 was released on June 17, 2016.

About
-----

Workflow is a Finite State Machine with memory.  It is used to execute
set of methods in a specified order.

Workflow was originally developed by Roman Chyla.  It is now being
maintained by the Invenio collaboration.

Incompatible changes
--------------------

- Drops `setVar()`, `getVar()`, `delVar()` and exposes the
  `engine.store` dictionary directly, with an added `setget()` method
  that acts as `getVar()`.
- Renames s/getCurrObjId/curr_obj_id/ and
  s/getCurrTaskId/curr_task_id/ which are now properties. Also renames
  s/getObjects/get_object/ which now no longer returns index.
- Removes PhoenixWorkflowEngine. To use its functionality, the new
  engine model's extensibility can be used.
- Moves `processing_factory` out of the `WorkflowEngine` and into its
  own class. The majority of its operations can now be overridden by
  means of subclassing `WorkflowEngine` and the new, complementing
  `ActionMapper` and `TransitionActions` classes and defining
  properties. This way `super` can be used safely while retaining the
  ability to `continue` or `break` out of the main loop.
- Moves exceptions to `errors.py`.
- Changes interface to use pythonic names and renames methods to use
  more consistent names.
- `WorkflowHalt` exception was merged into `HaltProcessing` and the
  `WorkflowMissingKey` exception has been dropped.
- Renames ObjectVersion to ObjectStatus (as imported from Invenio) and
  ObjectVersion.FINAL to ObjectVersion.COMPLETED.

New features
------------

- Introduces `SkipToken` and `AbortProcessing` from `engine_db`.
- Adds support for signaling other processes about the actions taken
  by the engine, if blinker is installed.
- Moves callbacks to their own class to reduce complexity in the
  engine and allow extending.
- `GenericWorkflowEngine.process` now supports restarting the workflow
  (backported from Invenio)

Improved features
-----------------

- Updates all `staticproperty` functions to `classproperty` to have
  access to class type and avoid issue with missing arguments to class
  methods.
- Re-raises exceptions in the engine so that they are propagated
  correctly to the user.
- Replaces `_i` with `MachineState`, which protects its contents and
  explains their function.
- Allows for overriding the logger with any python-style logger by
  defining the `init_logger` method so that projects can use their
  own.
- Splits the DbWorkflowEngine initializer into `with_name` and
  `from_uuid` for separation of concerns. The latter no longer
  implicitly creates a new object if the given uuid does not exist in
  the database. The uuid comparison with the log name is now
  reinforced.
- Updates tests requirements.

Installation
------------

   $ pip install workflow

Documentation
-------------

   http://workflow.readthedocs.org/en/v1.2.0

Good luck and thanks for using Workflow.

| Invenio Development Team
|   Email: info@invenio-software.org
|   IRC: #invenio on irc.freenode.net
|   Twitter: http://twitter.com/inveniosoftware
|   GitHub: https://github.com/inveniosoftware/workflow
