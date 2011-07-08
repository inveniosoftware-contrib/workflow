===============
Simple Workflow
===============


Introduction
============

I was looking for a workflow engine some time ago, and there weren't many for 
Python. Google will show up quite a few, but:

* They are Plone or Django or Project-X specific
* I found them too complicated (non-intuitive)
* or abandoned
* or any combination of the above...

So I created my own workflow engine (alas) - but it sort of works... quite well, 
so I haven't looked for a better alternative.


Details
=======

Wofklow engine is a Finite State Machine with memory
It is used to execute set of methods in a specified order.

Here is a simple example of a configuration:
    
    [
      check_token_is_wanted, # (run always)
      [                      # (run conditionally)
         check_token_numeric,
         translate_numeric,
         next_token          # (stop processing, continue with next token)
         ],
      [                      # (run conditionally)
         check_token_proper_name,
         translate_proper_name,
         next_token          # (stop processing, continue with next token)
         ],
      normalize_token,       # (only for "normal" tokens)
      translate_token,
    ]

You can probably guess what the processing pipeline does with tokens - the 
whole task is made of four steps and the whole configuration is just stored 
as a Python list. Every task is implemeted as a function that takes two objects:

* currently processed object 
* workflow engine instance
   
Example:

    def next_token(obj, eng):
        eng.ContinueNextToken()
   
There are NO explicit states, conditions, transitions - the job of the engine is 
simply to run the tasks one after another. It is the responsibility of the task 
to tell the engine what is going to happen next; whether to continue, stop, 
jump back, jump forward and few other options.

This is actually a *feature*, I knew that there will be a lot of possible 
exceptions and transition states to implement for NLP processing and I also 
wanted to make the workflow engine simple and fast -- but it has disadvantages,
you can make more errors and workflow engine will not warn you.

The workflow module comes with many patterns that can be directly used in the
definition of the pipeline, such as IF, IF_NOT, PARALLEL_SPLIT and others.

*This version requires Python 2 and many of the workflow patterns (such as IF,
XOR, WHILE) are implemented using lambdas, therefore not suitable for Python 3.*




The individual tasks then can influence the whole pipeline, available ''commands'' are:

    eng.stopProcessing #stops the current workflow
    eng.haltProcessing: #halts the workflow (can be used for nested wf engines)
    eng.continueNextToken # can be called many levels deep, jumps up to next token
    eng.jumpTokenForward # will skip the next object and continue with the next one
    eng.jumpTokenBack # will return back, start processing again
    eng.jumpCallForward #in one loop [call, call...] jumps x steps forward
    eng.jumpCallBack #in one loop [call, call...] jumps x steps forward
    eng.breakFromThisLoop #break from this loop, but do not stop processing


Consider this example of a task:

    def if_else(call):
        def inner_call(obj, eng):
           if call(obj, eng):     #if True, continue processing
              eng.jumpForward(1)
           else:                  #else, skip the next step
              eng.jumpForward(2)
        return inner_call

We can then write *workflow definition* like:


    if_else(stage_submission),
    [
        [if_else(fulltext_available),  #this will be run only when fulltext is uploaded during form submission
         [extract_metadata, populate_empty_fields],
         [#do nothing ]],
        [if_else(check_for_duplicates), 
         [stop_processing], 
         [synchronize_fields, replace_values]],
        check_mandatory_fields,]
        ],                             
        [
        check_mandatory_fields,        # this will run only for 'review' stage
        check_preferred_values,
        save_record
    ]
    


== Tasks ==

Tasks are simple python functions, we can enforce rules (not done yet!) in a pythonic 
way using pydoc conventions, consider this:


    def check_duplicate(obj, eng):
       """
       This task checks if the uploaded fulltext is a duplicate
            @type obj: InspireGeneralForm 
            @precondition: obj.paths[]
                    list, list of paths to uploaded files
            @postcondition: obj.fulltext[]
                    list containing txt for the extracted document
                            obj.duplicateids[]
                    list of inspire ids records that contain the duplicate of this document
            @raise: stopProcessing on error
            @return: True if duplicate found
                   
       """
       ...

So using the python docs, we can instruct workflow engine what types of arguments 
are acceptable, what is the expected outcome and what happens after the task finished. 
And let's say, there will be a testing framework which will run the workflow 
pipeline with fake arguments and will test all sorts of conditions. So, the 
configuration is not cluttered with states and transitions that are possible, 
developers can focus on implementation of the individual tasks, and site admins 
should have a good understanding what the task is supposed to do -- the description 
of the task will be displayed through the web GUI.

=== Some examples ===

Here are some examples of workflow patterns (images are from: http://www.yawlfoundation.org) 
and their implementation in Python. This gives you an idea that workflow engine 
remains very simple and by supplying special functions, we can implement different 
patterns.


.. image:: http://www.yawlfoundation.org/images/resources/patterns/basic_ps.jpg

This pattern is called Parallel split (as tasks B,C,D are all started in parallel 
after task A). It could be implemented like this:


    def PARALLEL_SPLIT(*args):
        """
        Tasks A,B,C,D... are all started in parallel
        @attention: tasks A,B,C,D... are not addressable, you can't
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
            lock=thread.allocate_lock()
            i = 0
            eng.setVar('lock', lock)
            for func in calls:
                new_eng = duplicate_engine_instance(eng)
                new_eng.setWorkflow([lambda o,e: e.setVar('lock', lock), func])
                thread.start_new_thread(new_eng.process, ([obj], ))
                #new_eng.process([obj])
        return lambda o, e: _parallel_split(o, e, args)

        
And is used like this:

    from workflow.patterns import PARALLEL_SPLIT
    from my_module_x import task_a,task_b,task_c,task_d
    
    [
     task_a, 
     PARALLEL_SPLIT(task_b,task_c,task_d)
    ]


=== Arbitrary cycle(s) ===

.. image:: http://www.yawlfoundation.org/images/resources/patterns/struc_arb.jpg

This is just for your amusement (and to see how complicated it looks in the 
configuration).


    #!python
    [
      ...        #here some conditional start
      task_a,
      task_b,
      task_c,
      if_else(some_test), 
        [task_d, [if_else(some_test), 
                    lambda obj, eng: eng.jumpCallBack(-6),  #jump back to task_a
                    some_other_task,  
                  ]]
        [some_other_task],
      ...
    ]

TODO: Jumping back and forward is obviously dangerous and tedious (depending on the 
actual configuration), we need a better solution.


=== Synchronization ===

..image:: http://www.yawlfoundation.org/images/resources/patterns/basic_synch.jpg

After the execution of task B, task C, and task D, task E can be executed 
(I will present the threaded version, as the sequential version would be dead simple). 

    
    def SYNCHRONIZE(*args, **kwargs):
        """
        After the execution of task B, task C, and task D, task E can be executed.
        @var *args: args can be a mix of callables and list of callables
                    the simplest situation comes when you pass a list of callables
                    they will be simply executed in parallel.
                       But if you pass a list of callables (branch of callables)
                    which is potentionally a new workflow, we will first create a
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


Configuration (ie. what would admins write):
    
    from workflow.patterns import SYNCHRONIZE
    from my_module_x import task_a,task_b,task_c,task_d
    
    [
     synchronize(task_b,task_c,task_d, task_a)
    ]


TODO:
=====

There already exist a web-based GUI for construction of the workflow, publish it!

Fix the bin/run-workflow.py script for executing the workflows.

Explain how the workflows can be saved and organized, embedded.
