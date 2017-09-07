==========
 Workflow
==========
.. currentmodule:: workflow

What is Workflow?
=================

The Workflow library provides a method of running Finite State Machines with
memory. It can be used to execute a set of methods, complete with conditions
and patterns.

Workflow allows for a number of independent pieces of data to be processed by
the same logic, while allowing for the entire process to be forwarded,
backwarded, paused, inspected, re-executed, modified and stored.

How do I use Workflow?
======================

In the following sections we will take a look at Workflow's features by working
on examples of increasing complexity. Please keep an eye out for comments in
the code as they provide crucial information.

Basic workflow use
------------------

Basic use is comprised of the following steps:

1. Instantiate a **workflow engine**. For this example we will use the simplest
   provided one, but you may also extend it to add custom behaviour.

.. code-block:: python

    from workflow.engine import GenericWorkflowEngine
    my_engine = GenericWorkflowEngine()

2. Create **tasks**. These are purpose-built function functions that the
   workflow engine can execute.

    The engine always passes **(current_token, current_engine)** as arguments
    to these functions, so they need to support them. Note the **add_data**
    function needs to be able to accept more arguments. For this we use a
    closure.

.. code-block:: python

    from functools import wraps

    def print_data(obj, eng):
        """Print the data found in the token."""
        print obj.data

    def add_data(number_to_add):
        """Add number_to_add to obj.data."""
        @wraps(add_data)
        def _add_data(obj, eng):
            obj.data += number_to_add
        return _add_data


2. Create a **workflow definition** (also known as **callbacks**). This is a
   (sometimes nested) list of **tasks** that we wish to run.

.. code-block:: python

    my_workflow_definition = [
        add_data(1),
        print_data
    ]

3. Define **tokens**. This is the data that we wish to feed the workflow. Since
   the data we will deal with in this example is immutable, we need to place it
   in **token wrappers** . Another reason you may wish to wrap your data is to
   be able to store `metadata` in the object.

.. code-block:: python

    class MyObject(object):
        def __init__(self, data):
            self.data = data

    my_object0 = MyObject(0)
    my_object1 = MyObject(1)

4. **Run** the engine on a list of such wrappers with our workflow definition.

    The engine passes the tokens that we give it one at a time through the
    workflow.

.. code-block:: python

    my_engine.callbacks.replace(my_workflow_definition)

    my_engine.process([my_object0, my_object1])
    # The engine prints: "1\n2"
    my_object0.data == 1
    my_object1.data == 2

5. **Bonus**! Once the engine has ran, it can be reused.

.. code-block:: python

    my_engine.process([my_object0, my_object1])
    # The engine prints: "2\n3"
    my_object0.data == 2
    my_object1.data == 3


Loops and interrupts
--------------------

Let's take a look at a slightly more advanced example. There are two things to
note here:

* How control flow is done. We provide, among others, **IF_ELSE** and
  **FOR** statements. They are simple functions - therefore you can make your
  own if you wish to. We will see examples of this in the Details section.

* Control flow can reach outside the engine via exceptions. We will raise the
  **WorkflowHalt** exception to return the control to our code before the
  workflow has even finished and then even resume it.

In this example, we have a series of lists composed of 0 and 1 and we want to:

    1. Add [0, 1] at the end of the list.
    2. Repeat a until list >= [0, 1, 0, 1].
    3. Add [1] when we are done.

Here are some example transformations that describe the above:

    * [] --> [0, 1, 0, 1, 1]
    * [0, 1] --> [0, 1, 0, 1, 1]
    * [0, 1, 0, 1] --> [0, 1, 0, 1, 0, 1, 1]

Time for some code! Let's start with the imports. Pay close attention as their
arguments are explained briefly here.

.. code-block:: python

    from workflow.engine import GenericWorkflowEngine
    from workflow.errors import HaltProcessing
    from workflow.patterns.controlflow import (
    FOR,        # Simple for-loop, a-la python. First argument is an iterable,
                # second defines where to save the current value, and the third
                # is the code that runs in the loop.

    HALT,       # Halts the engine. This brings it to a state where it can be
                # inspected, resumed, restarted, or other.

    IF_ELSE,    # Simple `if-else` statement that accepts 3 arguments.
                # (condition, tasks if true, tasks if false)

    CMP,        # Simple function to support python comparisons directly from a
                # workflow engine.
    )

Now to define some functions of our own.

Note that the first function leverages `eng.extra_data`. This is a simple
dictionary that the `GenericWorkflowEngine` exposes and it acts as a shared
storage that persists during the execution of the engine.

The two latter functions wrap engine functionality that's already there, but
add `print` statements for the example.

.. code-block:: python

    def append_from(key):
        """Append data from a given `key` of the engine's `extra_data`."""
        def _append_from(obj, eng):
            obj.append(eng.extra_data[key])
            print "new data:", obj
        return _append_from

    def interrupt_workflow(obj, eng):
        """Raise the `HaltProcessing` exception.

        This is not handled by the engine and bubbles up to our code.
        """
        print "Raising HaltProcessing"
        eng.halt("interrupting this workflow.")

    def restart_workflow(obj, eng):
        """Restart the engine with the current object, from the first task."""
        print "Restarting the engine"
        eng.restart('current', 'first')

We are now ready to create the workflow:

.. code-block:: python

    my_workflow = [
        FOR(range(2), "my_current_value", # For-loop, from 0 to 1, that sets
                                          # the current value to
                                          # `eng.extra_data["my_current_value"]`
            [
                append_from("my_current_value"),  # Gets the value set above
                                                  # and appends it to our token
            ]
        ), # END FOR

        IF_ELSE(
            CMP((lambda o, e: o), [0, 1 ,0, 1], "<"),  # Condition:
                                                       # "if obj < [0,1,0,1]:"

            [ restart_workflow ],                   # Tasks to run if condition
                                                    # is True:
                                                    # "return back to the FOR"

            [                                       # Tasks to run if condition
                                                    # is False:

            append_from("my_current_value"),        # "append 1 (note we still
                                                    # have access to it)
            interrupt_workflow                      # and interrupt"
            ]
        ) # END IF_ELSE
    ]

Because our workflow interrupts itself, we will wrap the call to `process` and
`restart`, in `try-except` statements.

.. code-block:: python

    # Create the engine as in the previous example
    my_engine = GenericWorkflowEngine()
    my_engine.callbacks.replace(my_workflow)

    try:
        # Note how we don't need to keep a reference to our tokens - the engine
        # allows us to access them via `my_engine.objects` later.
        my_engine.process([[], [0,1], [0,1,0,1]])
    except HaltProcessing:
        # Our engine was built to throw this exception every time an object is
        # completed. At this point we can inspect the object to decide what to
        # do next. In any case, we will ask it to move to the next object,
        # until it stops throwing the exception (which, in our case, means it
        # has finished with all objects).
        while True:
            try:
                # Restart the engine with the next object, starting from the
                # first task.
                my_engine.restart('next', 'first')
            except HaltProcessing:
                continue
            else:
                print "Done!", my_engine.objects
                break

Here is what the execution prints::

    new data: [0]
    new data: [0, 1]
    Restarting the engine
    new data: [0, 1, 0]
    new data: [0, 1, 0, 1]
    new data: [0, 1, 0, 1, 1]
    Raising HaltProcessing
    new data: [0, 1, 0]
    new data: [0, 1, 0, 1]
    new data: [0, 1, 0, 1, 1]
    Raising HaltProcessing
    new data: [0, 1, 0, 1, 0]
    new data: [0, 1, 0, 1, 0, 1]
    new data: [0, 1, 0, 1, 0, 1, 1]
    Raising HaltProcessing
    Done! [[0, 1, 0, 1, 1], [0, 1, 0, 1, 1], [0, 1, 0, 1, 0, 1, 1]]

Celery support
--------------

Celery is a widely used distributed task queue. The independent nature of
workflows and their ability to be restarted and resumed makes it a good
candidate for running in a task queue. Let's take a look at running a workflow
inside celery.

Assuming workflow is already installed, let's also install celery::

    $ pip install 'celery[redis]'

Onto the code next:

.. code-block:: python

    from celery import Celery

    # `app` is required by the celery worker.
    app = Celery('workflow_sample', broker='redis://localhost:6379/0')

    # Define a couple of basic tasks.
    def add(obj, eng):
        obj["value"] += 2

    def print_res(obj, eng):
        print obj.get("value")

    # Create a workflow out of them.
    flow = [add, print_res]

    # Mark our execution process as a celery task with this decorator.
    @app.task
    def run_workflow(data):
        # Note that the imports that this function requires must be done inside
        # it since our code will not be running in the global context.
        from workflow.engine import GenericWorkflowEngine
        wfe = GenericWorkflowEngine()
        wfe.setWorkflow(flow)
        wfe.process(data)

    # Code that runs when we call this script directly. This way we can start
    # as many workflows as we wish and let celery handle how they are
    # distributed and when they run.
    if __name__ == "__main__":
        run_workflow.delay([{"value": 10}, {"value": 20}, {"value": 30}])

Time to bring celery up:

    1. Save this file as `/some/path/workflow_sample.py`

    2. Bring up a worker in one terminal::

        $ cd /some/path
        $ celery -A workflow_sample worker --loglevel=info

    3. Use another terminal to request `run_workflow` to be ran with the above arguments::

        $ cd /some/path
        $ python workflow_sample.py

You should see the worker working. Try running `python workflow_sample.py`
again.

Storage-capable engine
----------------------

The Workflow library comes with an alternative engine which is built to work
with SQLAlchemy databases (`DbWorkflowEngine`). This means that one can store
the state of the engine and objects for later use. This opens up new
possibilities:

    * A front-end can be attached to the engine with ease.
    * Workflows can be stored for resume at a later time..
    * ..or even shared between processing nodes.

In this example we will see a simple implementation of such a database-stored,
resumable workflow.

We will reveal the problem that we will be solving much later. For now, we can
start by creating a couple of SQLAlchemy schemas:

    * One to attach to the workflow itself (a workflow will represent a student)
    * and one where a single grade (a grade will be the grade of a single test)

Note that the `Workflow` model below can store an element pointer. This pointer
(found at `engine.state.token_pos`) indicates the object that is currently being
processed and saving it is crucial so that the engine can resume at a later
time from that point.

.. code-block:: python

    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String, create_engine, ForeignKey, Boolean
    from sqlalchemy.orm import sessionmaker, relationship

    # Create an engine and a session
    engine = create_engine('sqlite://')
    Base = declarative_base(bind=engine)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()


    class Workflow(Base):
        __tablename__ = 'workflow'
        id = Column(Integer, primary_key=True)
        state_token_pos = Column(Integer, default=-1)
        grades = relationship('Grade', backref='workflow',
                               cascade="all, delete, delete-orphan")

        def save(self, token_pos):
            """Save object to persistent storage."""
            self.state_token_pos = token_pos
            session.begin(subtransactions=True)
            try:
                session.add(self)
                session.commit()
            except Exception:
                session.rollback()
                raise



    class Grade(Base):
        __tablename__ = 'grade'
        id = Column(Integer, primary_key=True)
        data = Column(Integer, nullable=False, default=0)
        user_id = Column(Integer, ForeignKey('workflow.id'))

        def __init__(self, grade):
            self.data = grade
            session.add(self)

    Base.metadata.create_all(engine)

Next, we have to tell `DbWorkflowEngine` how and when to use our storage. To do
that we need to know a bit about the engine's `processing_factory` property,
which is expected to provide this structure of methods and properties:

    * `before_processing`
    * `after_processing`
    * `before_object`
    * `after_object`

    * `action_mapper` (property)

        * `before_callbacks`
        * `after_callbacks`
        * `before_each_callback`
        * `after_each_callback`

    * `transition_exception_mapper` (property)

        * `StopProcessing`
        * `HaltProcessing`
        * `ContinueNextToken`
        * `JumpToken`
        * `Exception`
        * ... (Can be extended by adding any method that has the name of an
          expected exception)

The `transition_exception_mapper` can look confusing at first. It contains not
Exceptions, but methods that are called when exceptions with the same name are
raised.

    * Some exceptions are internal to the engine only and never bubble up. (eg
      `JumpToken`, `ContinueNextToken`)
    * Others are partly handled internally and then bubbled up to the user to
      take action. (eg `Exception`)

 Let's use the above to ask our engine to:

    1. Save the first objects that it is given.
    2. Save to our database every time it finished processing an object and
           when there is an expected failure.

For now, all we need to know is that in our example, `HaltProcessing` is an
exception that we will intentionally raise and we want to save the engine when
it occurs. Once again, follow the comments carefully to understand the code.

.. code-block:: python

    from workflow.engine_db import DbWorkflowEngine
    from workflow.errors import HaltProcessing
    from workflow.engine import TransitionActions, ProcessingFactory

    class MyDbWorkflowEngine(DbWorkflowEngine):

        def __init__(self, db_obj):
            """Load an old `token_pos` from the db into the engine."""

            # The reason we save token_pos _first_, is because calling `super`
            # will reset.
            token_pos = db_obj.state_token_pos

            self.db_obj = db_obj
            super(DbWorkflowEngine, self).__init__()

            # And now we inject it back into the engine's `state`.
            if token_pos is not None:
                self.state.token_pos = token_pos
            self.save()

        # For this example we are interested in saving `token_pos` as explained
        # previously, so we override `save` to do that.
        def save(self, token_pos=None):
            """Save the state of the workflow."""
            if token_pos is not None:
                self.state.token_pos = token_pos
            self.db_obj.save(self.state.token_pos)

        # We want our own processing factory, so we tell the engine that we
        # have subclassed it below.
        @staticproperty
        def processing_factory():
            """Provide a processing factory."""
            return MyProcessingFactory


    class MyProcessingFactory(ProcessingFactory):
        """Processing factory for persistence requirements."""

        # We also have our own `transition_actions`
        @staticproperty
        def transition_exception_mapper():
            """Define our for handling transition exceptions."""
            return MyTransitionActions

        # Before any processing is done, we wish to save the `objects` (tokens)
        # that have been passed to the engine, if they aren't already stored.
        @staticmethod
        def before_processing(eng, objects):
            """Make sure the engine has a relationship with
            its objects."""
            if not eng.db_obj.grades:
                for obj in objects:
                    eng.db_obj.grades.append(obj)

        # We wish to save on every successful completion of a token.
        @staticmethod
        def after_processing(eng, objects):
            """Save after we processed all the objects successfully."""
            eng.save()


    class MyTransitionActions(TransitionActions):

        # But we also wish to save when `HaltProcessing` is raised, because this
        # is going to be an expected situation.
        @staticmethod
        def HaltProcessing(obj, eng, callbacks, e):
            """Save whenever HaltProcessing is raised, so
            that we don't lose the state."""
            eng.save()
            raise e


And now, for the problem that we want to solve itself. Imagine an fictional
exam where a student has to take 6 tests in one day.  The tests are processed
in a specific order by a system. Whenever the system locates a failing grade,
as punishment, the student is asked to take the failed test again the next day.
Then the checking process is resumed until the next failing grade is located
and the student must show up again the following day.

Assume that a student, Zack P. Hacker, has just finished taking all 6 tests. A
workflow that does the following checking can now be implemented like so:

.. code-block:: python

    from workflow.patterns.controlflow import IF, HALT
    from workflow.utils import staticproperty

    my_workflow_instance = Workflow()
    my_db_engine = MyDbWorkflowEngine(my_workflow_instance)

    def grade_is_not_passing(obj, eng):
        print 'Testing grade #{0}, with data {1}'.format(obj.id, obj.data)
        return obj.data < 5

    callbacks = [
        IF(grade_is_not_passing,
            [
                HALT()
            ]),
    ]

    my_db_engine.callbacks.replace(callbacks)
    try:
        my_db_engine.process([
            Grade(6), Grade(5), Grade(4),
            Grade(5), Grade(2), Grade(6)
        ])
    except HaltProcessing:
        print 'The student has failed this test!'

    # At this point, the engine has already saved its state in the database,
    # regardless of the outcome.

The above script prints::

    Testing grade #1, with data 6
    Testing grade #2, with data 5
    Testing grade #3, with data 4
    The student has failed this test!

"Obviously this system is terrible and something must be done", thinks Zack who
was just notified about his "4" and logs onto the system, armed with a small
python script:

.. code-block:: python

    def amend_grade(obj, eng):
        print 'Amending this grade..'
        obj.data = 5

    evil_callbacks = [
        IF(grade_is_not_passing,
            [
                amend_grade
            ]),
    ]

    # Load yesterday's workflow and bring up an engine for it.
    revived_workflow = session.query(Workflow).one()
    my_db_engine = MyDbWorkflowEngine(revived_workflow)

    print '\nWhat Zak sees:', [grade.data for grade in revived_workflow.grades]

    # Let's fix that.
    my_db_engine.callbacks.replace(evil_callbacks)
    print 'Note how the engine resumes from the last failing test:'
    my_db_engine.restart('current', 'first', objects=revived_workflow.grades)

These words are printed in Zack's terminal::


    What Zak sees: [6, 5, 4, 5, 2, 6]
    Note how the engine resumes from the last failing test:
    Testing grade #3, with data 4
    Amending this grade..
    Testing grade #4, with data 5
    Testing grade #5, with data 2
    Amending this grade..
    Testing grade #6, with data 6


When someone logs into the system to check how Zack did..

.. code-block:: python

    revived_workflow = session.query(Workflow).one()
    print '\nWhat the professor sees:', [grade.data for grade in revived_workflow.grades]

Everything looks good::

    What the professor sees: [6, 5, 5, 5, 5, 6]

The moral of this story is to keep off-site logs and back-ups. Also, workflows
are complex but powerful.

.. DbWorkflowEngine interface
.. ~~~~~~~~~~~~~~~~~~~~~~~~~~

.. `DBWorkflowEngine` provides a more extended interface out of the box.

.. has a `save` method out of the box. By default it
.. calls `.save()` on `db_obj`, allowing for an optional argument:
.. `status`. `status` allows saving the status (eg HALTED) of the engine
.. the database. For simplicity, we do not support this in our
.. database. You can read more about the default status storage representations in the "

Signals support
===============

Adding to the exception and override-based mechanisms, Workflow supports a few
signals out of the box if the `blinker` package is installed. The following
exceptions are triggered by the `GenericWorkflowEngine`.

============================  =================================================
Signal                        Called by
============================  =================================================
`workflow_started`            ProcessingFactory.before_processing
`workflow_finished`           ProcessingFactory.after_processing
`workflow_halted`             TransitionActions.HaltProcessing
============================  =================================================

Useful engine methods
=====================

Other than `eng.halt`, the GenericWorkflowEngine provides more convenience
methods out of the box.

===============================  ==============================
Sample call                      Description
===============================  ==============================
`eng.stop()`                     stop the workflow
`eng.halt("list exhausted")`     halt the workflow
`eng.continue_next_token()`      continue from the next token
`eng.jump_token(-2)`             jump `offset` tokens
`eng.jump_call(3)`               jump `offset` steps of a loop
`eng.break_current_loop()`       break out of the current loop
===============================  ==============================

By calling these, any **task** can influence the whole pipeline. You can read
more about the methods our engines provide at the end of this document.


Patterns
========

The workflow module also comes with many patterns that can be directly used in
the definition of the pipeline, such as **PARALLEL_SPLIT**.

Consider this example of a task:

.. code-block:: python

    def if_else(call):
        def inner_call(obj, eng):
           if call(obj, eng):     # if True, continue processing..
              eng.jump_call(1)
           else:                  # ..else, skip the next step
              eng.jump_call(2)
        return inner_call

We can then write a **workflow definition** like this:

.. code-block:: python

    [
        if_else(stage_submission),
            [
                [
                    if_else(fulltext_available),
                        [extract_metadata, populate_empty_fields],
                        []
                ],
                [
                    if_else(check_for_duplicates),
                        [stop_processing],
                        [synchronize_fields, replace_values]
                ],
                check_mandatory_fields,
            ],
            [
                check_mandatory_fields,
                check_preferred_values,
                save_record
            ]
    ]

Example: Parallel split
-----------------------

.. raw:: html

    <img src="http://www.yawlfoundation.org/images/patterns/basic_ps.jpg"/>

This pattern is called Parallel split (as tasks B,C,D are all started in
parallel after task A). It could be implemented like this:

.. code-block:: python

    def PARALLEL_SPLIT(*args):
        """
        Tasks A,B,C,D... are all started in parallel
        @attention: tasks A,B,C,D... are not addressable,
            you can't use jumping to them (they are invisible to
            the workflow engine). Though you can jump inside the
            branches
        @attention: tasks B,C,D... will be running on their own
            once you have started them, and we are not waiting for
            them to finish. Workflow will continue executing other
            tasks while B,C,D... might be still running.
        @attention: a new engine is spawned for each branch or code,
            all operations works as expected, but mind that the branches
            know about themselves, they don't see other tasks outside.
            They are passed the object, but not the old workflow
            engine object
        @postcondition: eng object will contain lock (to be used
            by threads)
        """

        def _parallel_split(obj, eng, calls):
            lock = thread.allocate_lock()
            eng.store['lock'] = lock
            for func in calls:
                new_eng = eng.duplicate()
                new_eng.setWorkflow([lambda o, e: e.store.update({'lock': lock}), func])
                thread.start_new_thread(new_eng.process, ([obj], ))
        return lambda o, e: _parallel_split(o, e, args)


Subsequently, we can use PARALLEL_SPLIT like this.

.. code-block:: python

    from workflow.patterns import PARALLEL_SPLIT
    from my_module_x import task_a,task_b,task_c,task_d

    [
     task_a,
     PARALLEL_SPLIT(task_b,task_c,task_d)
    ]

Note that PARALLEL_SPLIT is already provided in
`workflow.patterns.PARALLEL_SPLIT`.

Example: Synchronisation
------------------------

.. raw:: html

    <img src="http://www.yawlfoundation.org/images/patterns/basic_synch.jpg"/>

After the execution of task B, task C, and task D, task E can be executed
(I will present the threaded version, as the sequential version would be
dead simple).

.. code-block:: python

    def SYNCHRONIZE(*args, **kwargs):
        """
        After the execution of task B, task C, and task D, task E can be executed.
        @var *args: args can be a mix of callables and list of callables
                    the simplest situation comes when you pass a list of callables
                    they will be simply executed in parallel.
                       But if you pass a list of callables (branch of callables)
                    which is potentially a new workflow, we will first create a
                    workflow engine with the workflows, and execute the branch in it
        @attention: you should never jump out of the synchronized branches
        """
        timeout = MAX_TIMEOUT
        if 'timeout' in kwargs:
            timeout = kwargs['timeout']

        if len(args) < 2:
            raise Exception('You must pass at least two callables')

        def _synchronize(obj, eng):
            queue = MyTimeoutQueue()
            #spawn a pool of threads, and pass them queue instance
            for i in range(len(args)-1):
                t = MySpecialThread(queue)
                t.setDaemon(True)
                t.start()

            for func in args[0:-1]:
                if isinstance(func, list) or isinstance(func, tuple):
                    new_eng = duplicate_engine_instance(eng)
                    new_eng.setWorkflow(func)
                    queue.put(lambda: new_eng.process([obj]))
                else:
                    queue.put(lambda: func(obj, eng))

            #wait on the queue until everything has been processed
            queue.join_with_timeout(timeout)

            #run the last func
            args[-1](obj, eng)
        _synchronize.__name__ = 'SYNCHRONIZE'
        return _synchronize


Configuration (i.e. what would admins write):

.. code-block:: python

    from workflow.patterns import SYNCHRONIZE
    from my_module_x import task_a,task_b,task_c,task_d

    [
        SYNCHRONIZE(task_b,task_c,task_d, task_a)
    ]

.. .. automodule:: workflow
..    :members:

GenericWorkflowEngine API
=========================

This documentation is automatically generated from Workflow's source code.

.. autoclass:: workflow.engine.GenericWorkflowEngine
   :members:

.. autoclass:: workflow.engine.MachineState
   :members:

.. autoclass:: workflow.engine.Callbacks
   :members:

.. autoclass:: workflow.engine._Signal
   :members:

DbWorkflowEngine API
====================

.. autoclass:: workflow.engine_db.DbWorkflowEngine
   :members:

.. autoclass:: workflow.engine_db.WorkflowStatus
   :members:

.. autoclass:: workflow.engine_db.ObjectStatus
   :members:

.. include:: ../CONTRIBUTING.rst


Release notes
---------------
Here you can find the `release notes`_.


Changelog
------------
Here you can find the `full changelog for this version`_.


License
=======

.. include:: ../LICENSE


.. _release notes: _static/RELEASE_NOTES.txt
.. _full changelog for this version: _static/CHANGELOG.txt
