#!/usr/bin/env python
#
# This file is part of Workflow.
# Copyright (C) 2011, 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

import glob
import six
import sys
import os
import imp
import logging
import traceback
import getopt

from workflow import engine as main_engine
from workflow.patterns import PROFILE


log = main_engine.get_logger('workflow.run-worklfow')


def run(selection,
        listwf=None,
        places=None,
        verbose=False,
        profile=None,
        **kwargs):
    """
Example usage: %prog -l
               %prog 1 [to select first workflow to run]

usage: %prog glob_pattern(s) [options]
-l, --listwf: list available workflows
-i, --places = places: list of glob patterns to search for workflows
                       (separate with commas!)
-p, --profile=profile: profile the workflow and save output as x
-v, --verbose: makes for a lot of output
"""

    workflows = set()

    for pattern in places:
        for file in glob.glob(pattern):
            if '__init__.py' not in file:
                workflows.add(os.path.abspath(file))
    for f in selection:
        if os.path.exists(f) and os.path.isfile(f):
            workflows.add(os.path.abspath(f))

    workflows = sorted(workflows)

    short_names = []
    for w in workflows:
        head, tail = os.path.split(w)
        short_names.append('%s/%s' % (os.path.split(head)[1], tail))

    if listwf:
        for i in range(len(workflows)):
            print "%d - %s" % (i, short_names[i])
        if not len(workflows):
            log.warning(
                'No workflows found using default search path: \n%s' % (
                    '\n'.join(places)))

    if workflows:
        for s in selection:
            try:
                id = int(s)
                if verbose:
                    run_workflow(workflows[id],
                                 engine=TalkativeWorkflowEngine,
                                 profile=profile)
                else:
                    run_workflow(workflows[id], profile=profile)
            except ValueError:
                ids = find_workflow(workflows, os.path.normpath(s))
                if len(ids) == 0:
                    raise Exception("I found no wf for this id: %s" % (s, ))
                elif len(ids) > 1:
                    raise Exception(
                        "There is more than one wf for this id: %s (%s)" % (
                            s, ids))
                else:
                    if verbose:
                        run_workflow(workflows[ids[0]],
                                     engine=TalkativeWorkflowEngine,
                                     profile=profile)
                    else:
                        run_workflow(workflows[ids[0]], profile=profile)


def find_workflow(workflows, name):
    candidates = []
    i = 0
    for wf_name in workflows:
        if name in wf_name:
            candidates.append(i)
        i += 1
    return candidates


def run_workflow(file_or_module,
                 data=None,
                 engine=None,
                 profile=None):
    """Run the workflow
    @var file_or_module: you can pass string (filepath) to the
        workflow module, the module will be loaded as an anonymous
        module (from the file) and <module>.workflow will be
        taken for workflow definition
            You can also pass definition of workflow tasks in a
        list.
            If you pass anything else than we will consider it to be
        an object with attribute .workflow - that will be used to
        run the workflow (causing error if workflow attr not exists).
        If this object has an attribute .data it will be understood
        as another workflow engine definition, and data will be
        executed in a separated wfe, then results sent to the first
        wfe - the .data wfe will receive [{}] as input.

    @var data: data to feed into the workflow engine. If you pass
        data, then data defined in the workflow module are ignored
    @keyword engine: class that should be used to instantiate the
        workflow engine, default=GenericWorkflowEngine
    @group callbacks: standard engine callbacks
    @keyword profile: filepath where to save the profile if we
        are requested to run the workflow in the profiling mode
    @return: workflow engine instance (after its workflow was executed)
    """

    if isinstance(file_or_module, six.string_types):
        log.info("Loading: %s" % file_or_module)
        workflow = get_workflow(file_or_module)
    elif isinstance(file_or_module, list):
        workflow = WorkflowModule(file_or_module)
    else:
        workflow = file_or_module

    if workflow:
        if profile:
            workflow_def = PROFILE(workflow.workflow, profile)
        else:
            workflow_def = workflow.workflow
        we = create_workflow_engine(workflow_def,
                                    engine)
        if data is None:
            data = [{}]
            # there is a separate workflow engine for getting data
            if hasattr(workflow, 'data'):
                log.info('Running the special data workflow in a separate WFE')
                datae = create_workflow_engine(workflow.data,
                                               engine)
                datae.process(data)
                if data[0]:  # get prepared data
                    data = data[0]

        log.info('Running the workflow')
        we.process(data)
        return we
    else:
        raise Exception('No workfow found in: %s' % file_or_module)


def create_workflow_engine(workflow,
                           engine=None):
    """Instantiate engine and set the workflow and callbacks
    directly
    @var workflow: normal workflow tasks definition
    @keyword engine: class of the engine to create WE

    @return: prepared WE
    """
    if engine is None:
        engine = main_engine.GenericWorkflowEngine
    wf = engine()
    wf.setWorkflow(workflow)
    return wf


def get_workflow(file):
    """ Initializes module into a separate object (not included in sys) """
    name = 'XXX'
    x = imp.new_module(name)
    x.__file__ = file
    x.__id__ = name
    x.__builtins__ = __builtins__

    # XXX - chdir makes our life difficult, especially when
    # one workflow wrap another wf and relative paths are used
    # in the config. In such cases, the same relative path can
    # point to different locations just because location of the
    # workflow (parts) are different
    # The reason why I was using chdir is because python had
    # troubles to import files that containes non-ascii chars
    # in their filenames. That is important for macros, but not
    # here.

    # old_cwd = os.getcwd()

    try:
        # filedir, filename = os.path.split(file)
        # os.chdir(filedir)
        execfile(file, x.__dict__)
    except Exception, excp:
        sys.stderr.write(traceback.format_exc())
        log.error(excp)
        log.error(traceback.format_exc())
        return

    return x


def import_workflow(workflow):
    """Import workflow module
    @var workflow: string as python import, eg: merkur.workflow.load_x"""
    mod = __import__(workflow)
    components = workflow.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


class TalkativeWorkflowEngine(main_engine.GenericWorkflowEngine):
    counter = 0

    def __init__(self, *args, **kwargs):
        main_engine.GenericWorkflowEngine.__init__(self, *args, **kwargs)
        self.log = main_engine.get_logger(
            'TalkativeWFE<%d>' % TalkativeWorkflowEngine.counter)
        TalkativeWorkflowEngine.counter += 1

    def execute_callback(self, callback, obj):
        obj_rep = []
        max_len = 60

        def val_format(v):
            return '<%s ...>' % repr(v)[:max_len]

        def func_format(c):
            return '<%s ...%s:%s>' % (
                c.__name__,
                c.func_code.co_filename[-max_len:],
                c.func_code.co_firstlineno)
        if isinstance(obj, dict):
            for k, v in obj.items():
                obj_rep.append('%s:%s' % (k, val_format(v)))
            obj_rep = '{%s}' % (', '.join(obj_rep))
        elif isinstance(obj, list):
            for v in obj:
                obj_rep.append(val_format(v))
            obj_rep = '[%s]' % (', '.join(obj_rep))
        else:
            obj_rep = val_format(obj)
        self.log.debug('%s ( %s )' % (func_format(callback), obj_rep))
        callback(obj, self)


class WorkflowModule(object):

    """Workflow wrapper."""

    def __init__(self, workflow):
        self.workflow = workflow


def usage():
    print """
usage: %(prog)s [options] <workflow name or pattern>

examples:
  %(prog)s -l
      - to list the available workflows
  %(prog)s 1
      - to run the first workflow in the list

options:
-l, --list: list available workflows
-p, --places = places: list of glob patterns where the workflows
    are searched, example: ./this-folder/*.py,./that/*.pyw
    (separate with commas!)
-o, --profile=profile: profile the workflows, be default it saves
    output into tmp folder/profile.out
-v, --verbose: workflows are executed as talkative
-e, --level = (int): sets the verbose level, the higher the level,
    the less messages are printed
-h, --help: this help message

""" % {'prog': os.path.basename(__file__)}


def main():

    try:
        opts, args = getopt.getopt(sys.argv[1:], "lp:o:ve:h", [
            'list', 'places=', 'profile=', 'verbose', 'Vlevel=', 'help'])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    kw_args = {}
    output = None
    verbose = False
    for o, a in opts:
        if o in ("-v", '--verbose'):
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-e", "--level"):
            try:
                level = int(a)
                main_engine.set_global_level(level)
                main_engine.reset_all_loggers(level)
            except:
                print 'The argument to verbose must be integer'
                sys.exit(2)
        elif o in ('-v', '--verbose'):
            kw_args['verbose'] = True
        elif o in ('-p', '--places'):
            kw_args['places'] = a.split(',')
        elif o in ('-l', '--list'):
            kw_args['listwf'] = True
        else:
            assert False, "unhandled option %s" % o

    if (not len(args) or not len(opts)) and 'listwf' not in kw_args:
        usage()
        sys.exit()

    if 'places' not in kw_args:
        d = os.path.dirname(os.path.abspath(__file__))
        kw_args['places'] = ['%s/workflows/*.py' % d,
                             '%s/workflows/*.pyw' % d,
                             '%s/workflows/*.cfg' % d]

    run(args, **kw_args)


if __name__ == "__main__":
    main()
