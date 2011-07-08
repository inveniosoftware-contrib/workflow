
import glob
import sys
import os
import imp
import logging
import traceback

import merkur
from newseman.libs import optionparse
from newseman.general.workflow.engine import GenericWorkflowEngine
from merkur.box.utils.code import profiler


log = merkur.get_logger('merkur.runwf')

def run(selection,
        listwf=None,
        places=None,
        verbose=False,
        profile=None,
        **kwargs):
    '''
Example usage: %prog -l
               %prog 1 [to select first workflow to run]

usage: %prog glob_pattern(s) [options]
-l, --listwf: list available workflows
-i, --places = places: list of glob patterns to search for workflows (separate with commas!)
-p, --profile=profile: profile the workflow and save output as x
-v, --verbose: makes for a lot of output
'''

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
        return

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
                    raise Exception("There is more than one wf for this id: %s (%s)" % (s, ids))
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
                 processing_factory = None,
                 callback_chooser = None,
                 before_processing = None,
                 after_processing = None,
                 profile = None):
    """Runs the workflow
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
        @keyword processing_factory:
        @keyword callback_chooser:
        @keyword before_processing:
        @keyword after_processing:
    @keyword profile: filepath where to save the profile if we
        are requested to run the workflow in the profiling mode
    @return: workflow engine instance (after its workflow was executed)
    """

    if isinstance(file_or_module, basestring):
        log.info("Loading: %s" % file_or_module)
        workflow = get_workflow(file_or_module)
    elif isinstance(file_or_module, list):
        workflow = WorkflowModule(file_or_module)
    else:
        workflow = file_or_module


    if workflow:
        if profile:
            workflow_def = profiler(workflow.workflow, profile)
        else:
            workflow_def = workflow.workflow
        we = create_workflow_engine(workflow_def,
                                    engine,
                                    processing_factory,
                                    callback_chooser,
                                    before_processing,
                                    after_processing)
        if data is None:
            data = [{}]
            # there is a separate workflow engine for getting data
            if hasattr(workflow, 'data'):
                log.info('Running the special data workflow in a separate WFE')
                datae = create_workflow_engine(workflow.data,
                                               engine,
                                               processing_factory,
                                               callback_chooser,
                                               before_processing,
                                               after_processing)
                datae.process(data)
                if data[0]: # get prepared data
                    data = data[0]

        log.info('Running the workflow')
        we.process(data)
        return we
    else:
        raise Exception('No workfow found in: %s' % file_or_module)

def create_workflow_engine(workflow,
                           engine=None,
                           processing_factory = None,
                           callback_chooser = None,
                           before_processing = None,
                           after_processing = None):
    """Instantiate engine and set the workflow and callbacks
    directly
    @var workflow: normal workflow tasks definition
    @keyword engine: class of the engine to create WE
    @keyword processing_factory: WE callback
    @keyword callback_chooser: WE callback
    @keyword before_processing: WE callback
    @keyword after_processing: WE callback

    @return: prepared WE
    """
    if engine is None:
        engine = GenericWorkflowEngine
    wf = engine(processing_factory, callback_chooser, before_processing, after_processing)
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
        #filedir, filename = os.path.split(file)
        #os.chdir(filedir)
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

def main():
    if len(sys.argv ) > 1 and sys.argv[1] == 'demo':
        a = ''
        sys.argv[1:] = a.split()

    options, args =optionparse.parse(run.__doc__)
    options = options.__dict__

    if not len(args) or not options: options['listwf'] = True

    if options['verbose']:
        log.setLevel(logging.DEBUG)

    if not options['places']:
        d = os.path.dirname(os.path.abspath(__file__))
        options['places'] = ['%s/workflows/*.py' % d, '%s/workflows/*.pyw' % d, '%s/workflows/*.cfg' % d]

    run(args, **options)




class TalkativeWorkflowEngine(GenericWorkflowEngine):
    counter = 0
    def __init__(self, *args, **kwargs):
        GenericWorkflowEngine.__init__(self, *args, **kwargs)
        self.log = merkur.get_logger('TalkativeWFE<%d>' % TalkativeWorkflowEngine.counter)
        TalkativeWorkflowEngine.counter += 1

    def execute_callback(self, callback, obj):
        obj_rep = []
        max_len = 60
        def val_format(v):
            return '<%s ...>' % repr(v)[:max_len]
        def func_format(c):
            return '<%s ...%s:%s>' % (c.func_name, c.func_code.co_filename[-max_len:], c.func_code.co_firstlineno)
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
    """This is used just as a replacement for when module is needed but workflow
    was supplied directly"""
    def __init__(self, workflow):
        self.workflow = workflow

if __name__ == "__main__":
    main()

