import sys
import json
import types
from os import path

import artifacts
from templates import TaskTemplate

class ConfigError(Exception):
  def __init__(self, message):
    self.message = message

  def __str__(self):
    return repr(self.message)

class DatamakeConfig(object):
  def __init__(self):
    self.config = []

  def load(self, input_file):
    if isinstance(input_file, types.FileType):
      self.default_namespace = path.basename(input_file.name).split('.')[0]
    else:
      # Sources other than file, eg. tests use StringIO
      self.default_namespace = ''
    self.config = json.load(input_file)

  def load_from_file(self, input_filename):
    self.load(open(input_filename))

  def task_templates(self, override_parameters={}):
    task_templates = []
    if not isinstance(self.config, types.DictType):
      raise ConfigError("Config root should be json object")

    try:
      version = self.config['version']
      namespace = self.config.get('namespace', self.default_namespace)
      if version == '1.0':
        for task_info in self.config['tasks']:
          id = task_info['id']
          command = task_info.get('command', None)
          artifact = task_info.get('artifact', None)
          verify = task_info.get('verify', None)
          rollback = task_info.get('rollback', None)
          cleanup = task_info.get('cleanup', False)
          max_attempts = task_info.get('max_attempts', 1)
          parameters = task_info.get('parameters', {})
          dependencies = task_info.get('dependencies', [])

          parameters.update(override_parameters)
          
          template = TaskTemplate(namespace=namespace, id=id, command=command, artifact=artifact, 
            verify=verify, rollback=rollback,
            cleanup=cleanup, parameters=parameters, max_attempts=max_attempts, 
            dependencies=dependencies)
          task_templates.append(template)
      else:
        raise ConfigError("Unknown version {0}".format(version))
    except KeyError, e:
      raise ConfigError("Key error, could not find {0}".format(str(e)))

    return task_templates
