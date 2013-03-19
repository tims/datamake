import sys, json
import artifacts
import types
from tasks import TaskGraph, TaskTemplate

class ConfigError(Exception):
  def __init__(self, message):
    self.message = message

  def __str__(self):
    return repr(self.message)

class DatamakeConfig(object):
  def __init__(self):
    self.config = []
    self.override_parameters = {}

  def load(self, input_file):
    self.config = json.load(input_file)

  def load_from_file(self, input_filename):
    self.load(open(input_filename))

  def task_graph(self):
    task_graph = TaskGraph()

    if not isinstance(self.config, types.DictType):
      raise ConfigError("Config root should be json object")

    try:
      version = self.config['version']
      if version == '1.0':
        for task_info in self.config['tasks']:
          id = task_info['id']
          command = task_info.get('command', None)
          artifact = task_info.get('artifact', None)
          cleanup = task_info.get('cleanup', False)
          max_attempts = task_info.get('max_attempts', 1)
          parameters = task_info.get('parameters', {})
          dependencies = task_info.get('dependencies', [])

          parameters.update(self.override_parameters)
          
          task_template = TaskTemplate(id=id, command=command, artifact=artifact, cleanup=cleanup, parameters=parameters, max_attempts=max_attempts)
          task_graph.add_task_template(task_template)
          for upstream_task_id in dependencies:
            task_graph.add_task_dependency(upstream_task_id, task_template.id)
        return task_graph
      else:
        raise ConfigError("Unknown version {0}".format(version))
    except KeyError, e:
      raise ConfigError("Key error, could not find {0}".format(str(e)))
  