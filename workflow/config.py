# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""
Provide a class for reading global configuration options.

The reader in itself is using configobj to access the ini files. The reader
should be initialized (from the project root) with the path of the folder where
configuration files live.
"""

import inspect
import os
import sys
import traceback

from configobj import Section, OPTION_DEFAULTS, ConfigObjError, ConfigObj


class CustomConfigObj(ConfigObj):

    """Add support for key lookup in parent configuration.

    This is very small change into the default ``ConfigObj`` class
    - the only difference is in the ``parent_config`` parameter, if passed
    we will add it to the new instance and interpolation will use the
    values of the ``parent_config`` for lookup.
    """

    def __init__(self, infile=None, options=None, configspec=None,
                 encoding=None, interpolation=True, raise_errors=False,
                 list_values=True, create_empty=False, file_error=False,
                 stringify=True, indent_type=None, default_encoding=None,
                 unrepr=False, write_empty_values=False, _inspec=False,
                 parent_config=None):
        """Parse a config file or create a config file object."""
        self._inspec = _inspec
        # init the superclass
        # this is the only change - we pass the parent configobj if
        # available, to have lookup use its values
        Section.__init__(self, parent_config or self, 0, self)

        infile = infile or []
        if options is not None:
            import warnings
            warnings.warn('Passing in an options dictionary to ConfigObj() ',
                          'is deprecated. Use **options instead.',
                          DeprecationWarning, stacklevel=2)

        _options = {'configspec': configspec,
                    'encoding': encoding, 'interpolation': interpolation,
                    'raise_errors': raise_errors, 'list_values': list_values,
                    'create_empty': create_empty, 'file_error': file_error,
                    'stringify': stringify, 'indent_type': indent_type,
                    'default_encoding': default_encoding, 'unrepr': unrepr,
                    'write_empty_values': write_empty_values}

        options = dict(options or {})
        options.update(_options)

        # XXXX this ignores an explicit list_values = True in combination
        # with _inspec. The user should *never* do that anyway, but still...
        if _inspec:
            options['list_values'] = False

        defaults = OPTION_DEFAULTS.copy()
        # TODO: check the values too.
        for entry in options:
            if entry not in defaults:
                raise TypeError('Unrecognised option "%s".' % entry)

        # Add any explicit options to the defaults
        defaults.update(options)
        self._initialise(defaults)
        configspec = defaults['configspec']
        self._original_configspec = configspec
        self._load(infile, configspec)


class ConfigReader(object):

    """Facilitate easy reading/access to the INI style configuration.

    Modules/workflows should not import this, but config instance

    .. code-block:: python

        from workflow import config

    During instantion, reader loads the global config file - usually from
    ``./cfg/global.ini`` The values will be accessible as attributes, eg:
    ``reader.BASEDIR reader.sectionX.VAL``

    When workflow/module is accessing an attribute, the reader will also load
    special configuration (workflow-specific configuration) which has the same
    name as a workflow/module.

    Example: workflow 'load_seman_components.py'

    .. code-block:: python

        from merkur.config import reader
        reader.LOCAL_VALUE

        # at this moment, reader will check if exists
        # %basedir/etc/load_seman_components.ini if yes, the reader will load
        # the configuration and store it inside ._local if no LOCAL_VALUE
        # exists, error will be raised if in the meantime, some other module
        # imported reader and tries to access an attribute, the reader will
        # recognize the caller is different, will update the local config and
        # will server workflow-specifi configuration automatically

    You can pass a list of basedir folders - in that case, only the last one
    will be used for lookup of local configurations, but the global values will
    be inherited from all global.ini files found in the basedir folders.
    """

    def __init__(self, basedir=os.path.abspath(os.path.dirname(__file__)),
                 caching=True):
        """Initialize configuration reader."""
        object.__init__(self)
        self._local = {}
        self._global = {}
        self._on_demand = {}
        self._recent_caller = ''
        self._caching = caching
        self._main_config = None
        self._basedir = []

        self.setBasedir(basedir)

        # load configurations
        if isinstance(basedir, list) or isinstance(basedir, tuple):
            files = []
            for d in self._basedir:
                if os.path.exists(d):
                    files.append(
                        os.path.abspath(os.path.join(d, 'global.ini')))
            self.update(files)
        else:
            self.update()

    def __getattr__(self, key):
        """Return configuration value.

        1. First lookup in the local values;
        2. then in the global values.
        """
        # first find out who is trying to access us
        frame = inspect.currentframe().f_back

        # TODO - make it try the hierarchy first?
        if frame:
            cfile = self._getCallerPath(frame)
            if cfile:
                caller = self._getCallerName(cfile)
                if caller != self._recent_caller:
                    # TODO: make it optional, allow for read-once-updates
                    self.update_local(caller)  # update config

        if key in self._local:
            return self._local[key]
        elif key in self._global:
            return self._global[key]  # raise error ok
        else:
            global_cfg_path = self._main_config and os.path.abspath(
                self._main_config.filename) or 'None'
            local_cfg_path = self._findConfigPath(self._recent_caller)
            raise AttributeError(
                'Attribute "%s" not defined\n'
                'global_config: %s\nlocal_config: %s' % (
                    key, global_cfg_path, local_cfg_path))

    def _getCallerId(self, frame):
        if frame:
            cfile = self._getCallerPath(frame)
            if cfile:
                caller = self._getCallerName(cfile)
                return caller

    def getBaseDir(self):
        """Get basedir path."""
        return self._basedir

    def setBasedir(self, basedir):
        """Set a new basedir path.

        This is a root of the configuration directives from which other paths
        are resolved.
        """
        if not (isinstance(basedir, list) or isinstance(basedir, tuple)):
            basedir = [basedir]
        new_base = []
        for b in basedir:
            b = os.path.abspath(b)
            if b[0] != '\\':
                b = b.replace('\\', '/')
            b = b[0].lower() + b[1:]
            if b not in new_base:
                new_base.append(b)
        self._basedir = new_base
        self.update()

    def update(self, files=None, replace_keys={}):
        """Update values reading them from the main configuration file(s).

        :param files: list of configuration files
            (if empty, default file is read)
        :param replace_keys: dictionary of values that you want to replace
            this allows you to change config at runtime, but IT IS NOT
            RECOMMENDED to change anything else than global values (and you can
            change only top level values). If you don't know what you are
            doing, do not replace any keys!
        """
        if files is None:
            files = self._makeAllConfigPaths('global')

        updated = 0
        for file in files:
            if os.path.exists(file):
                # if we have more files, we will wrap/inherit them into one
                # object this object should not be probably usef for writing
                config = self._main_config = CustomConfigObj(
                    file, encoding='UTF8', parent_config=self._main_config
                )
                if replace_keys:
                    for k, v in replace_keys.items():
                        if k in config:
                            config[k] = v
                self._update(self._global, config)
                updated += 1
        return updated

    def init(self, filename):
        """Initialize configuration file."""
        if not os.path.exists(filename):
            filename = self._findConfigPath(filename)
        caller = self._getCallerId(inspect.currentframe().f_back)
        if not(self.update_local(caller, filename)):
            raise Exception('Config file: %s does not exist' % filename)

    def update_local(self, name, file=None):
        """Update the local, workflow-specific cache.

        :param name: name of the calling module (without suffix)
        :param file: file to load config from (if empty, default ini file
            will be sought)
        """
        self._recent_caller = name
        self._local = {}
        if file is None:
            file = self._findConfigPath(name)

        if file and os.path.exists(file):
            config = CustomConfigObj(file,
                                     encoding='UTF8',
                                     parent_config=self._main_config)

            self._update(self._local, config)
            return True

    def load(self, cfgfile, force_reload=False, failonerror=True,
             replace_keys={}):
        """Load configuration file on demand.

        :param cfgfile: path to the file, the path may be relative, in
            that case we will try to guess it using set basedir. Or it
            can be absolute
        :param force_reload: returns cached configuration or reloads
            it again from file if force_reload=True
        :param failonerror: bool, raise Exception when config file
            is not found/loaded
        :return: config object or None

        example:
        c = config.load('some-file.txt')
        c.some.key
        """
        realpath = None
        if os.path.exists(cfgfile):
            realpath = cfgfile
        else:
            new_p = self._findConfigPath(cfgfile)
            if new_p:
                realpath = new_p
            else:
                new_p = self._findConfigPath(cfgfile.rsplit('.', 1)[0])
                if new_p:
                    realpath = new_p

        if not realpath:
            if failonerror:
                raise Exception('Cannot find: %s' % cfgfile)
            else:
                sys.stderr.write('Cannot find: %s' % cfgfile)
                return

        if realpath in self._on_demand and not force_reload:
            return ConfigWrapper(realpath, self._on_demand[realpath])

        try:
            config = CustomConfigObj(realpath,
                                     encoding='UTF8',
                                     parent_config=self._main_config)
            if replace_keys:
                for k, v in replace_keys.items():
                    if k in config:
                        config[k] = v
        except ConfigObjError as msg:
            if failonerror:
                raise ConfigObjError(msg)
            else:
                self.traceback.print_exc()
                return

        self._on_demand[realpath] = {}
        self._update(self._on_demand[realpath], config)
        return ConfigWrapper(realpath, self._on_demand[realpath])

    def get(self, key, failonerror=True):
        """Get value from the key identified by string, eg. `index.dir`."""
        parts = key.split('.')
        pointer = self
        try:
            for p in parts:
                pointer = getattr(pointer, p)
            return pointer
        except (KeyError, AttributeError):
            global_cfg_path = self._main_config and os.path.abspath(
                self._main_config.filename) or 'None'
            local_cfg_path = self._findConfigPath(self._recent_caller)
            m = ('Attribute "%s" not defined\nglobal_config: %s\n'
                 'local_config: %s' % (key, global_cfg_path, local_cfg_path))
            if failonerror:
                raise AttributeError(m)
            else:
                sys.stderr.write(m)

    def _getCallerPath(self, frame):
        cfile = os.path.abspath(inspect.getfile(frame)).replace('\\', '/')
        f = __file__.replace('\\', '/')
        if f != cfile:
            return cfile

    def _getCallerName(self, path):
        cfile = os.path.split(path)[1]
        return cfile.rsplit('.', 1)[0]

    def getCallersConfig(self, failonerror=True):
        """Get the value from the calling workflow configuration.

        This is useful if we want to access configuration of the object
        that included us.

        :param key: name of the key to access, it is a string in a dot notation
        """
        # first find out who is trying to access us
        caller = ''
        frame = inspect.currentframe().f_back
        frame = inspect.currentframe().f_back
        if frame:
            frame = frame.f_back
            if frame:
                caller = self._getCallerName(self._getCallerPath(frame))
                path = self._findConfigPath(caller)
                if path:
                    config = self.load(path)
                    if config:
                        return config
        if failonerror:
            raise Exception('Error, cannot find the caller')

    def _findConfigPath(self, name):
        """Find the most specific config path."""
        for path in reversed(self._makeAllConfigPaths(name)):
            if os.path.exists(path):
                return path

    def _makeAllConfigPaths(self, name):
        f = []
        for d in self._basedir:
            path = '%s/%s.ini' % (d, name)
            f.append(path.replace('\\', '/'))
        return f

    def _update(self, pointer, config):
        for key, cfg_val in config.items():
            if isinstance(cfg_val, Section):
                o = cfgval()
                for k, v in cfg_val.items():
                    if isinstance(v, Section):
                        o2 = cfgval()
                        o.__setattr__(k, o2)
                        self._update(o2, v)
                    else:
                        o.__setattr__(k, v)
                pointer[key] = o
            else:
                pointer[key] = cfg_val

    def __str__(self):
        """Return textual representation of the current config.

        It can be used for special purposes (i.e. to save values
        somewhere and reload them -- however, they will be a simple
        dictionaries of textual values; without special powers. This
        class also does not provide ways to load such dumped values,
        we would be circumventing configobj and that is no good.
        """
        return ("{{'global_config' : {0._global}, "
                "'local_config' : {0._local}, "
                "'on_demand_config': {0._on_demand}, "
                "'recent_caller' : '{0._recent_caller}'}}".format(self))


class cfgval(dict):

    """Wrapper for configuration value."""

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__

    def __repr__(self):
        """Return representation of :class:`cfgval` instance."""
        # return '{%s}' % ',\n'.join(map(lambda o: "'.%s': %s" % (o[0],
        # repr(o[1])), self.__dict__.items()))
        return '%s\n%s' % ('#cfgwrapper', repr(self))


class ConfigWrapper(object):

    """Configuration wrapper."""

    def __init__(self, realpath, config):
        """Set instance `realpath` and `config` values."""
        self.__dict__['_config'] = config
        self.__dict__['_realpath'] = realpath

    def __getattr__(self, key):
        """Return value from instance `config` value."""
        return self._config[key]

    def __setattr__(self, key, value):
        """Store value in instance `config` dictionary."""
        self._config.__setitem__(key, value)

    def get(self, key):
        """Allow recursive dotted key access to configuration."""
        parts = key.split('.')
        pointer = self
        for p in parts:
            pointer = getattr(pointer, p)
        return pointer

    def __str__(self):
        """Return string representation with `config` and `realpath`."""
        return "%s #config from: %s" % (self._config, self._realpath)


# The config instance is a configuration reader
# The configuration can sit in different places

__cfgdir = None
# set by environmental variable
if 'WORKFLOWCFG' in os.environ:
    __cfgdir = os.environ['WORKFLOWCFG']
    if os.pathsep in __cfgdir:
        __cfgdir = __cfgdir.split(os.pathsep)
else:
    __cfgdir = [os.path.abspath(os.path.dirname(__file__)),
                os.path.abspath(os.path.dirname(__file__) + '/cfg')]


# This instance has access to all global/local config options
config_reader = ConfigReader(basedir=__cfgdir)
