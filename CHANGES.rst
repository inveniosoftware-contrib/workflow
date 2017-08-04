Changes
=======

Version 2.0.1 (released 2017-08-04):

Improved features
-----------------

- Replaces some deprecated calls with their non-deprecated equivalents
  in order to avoid raising a `DeprecationWarning`.

Version 2.0.0 (released 2016-06-17):

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

Version 1.2.0 (released 2014-10-23):

- Fix interference with the logging level. (#22 #23)
- Test runner is using Pytest. (#21)
- Python 3 support. (#7)
- Code style follows PEP8 and PEP257. (#6 #14)
- Improved Sphinx documentation. (#5 #28)
- Simplification of licensing. (#27)
- Spelling mistake fixes. (#26)
- Testing with Tox support. (#4)
- Configuration for Travis-Cl testing service. (#3)
- Test coverage report. (#2)
- Unix style line terminators. (#10)

Version 1.0 (released 2011-07-07):

- Initial public release.
- Includes the code created by Roman Chyla, the core of the workflow
  engine together with some basic patterns.
- Raja Sripada <rsripada at cern ch> contributed improvements to the
  pickle&restart mechanism.
