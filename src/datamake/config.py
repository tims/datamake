import sys, json
import artifacts
from task import TaskGraph, TaskTemplate

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

    for task_info in self.config:
      id = task_info['id']
      command = task_info.get('command', None)
      artifact = task_info.get('artifact', None)
      cleanup = task_info.get('cleanup', False)
      parameters = task_info.get('parameters', {})
      dependencies = task_info.get('dependencies', [])

      parameters.update(self.override_parameters)
      
      task_template = TaskTemplate(id=id, command=command, artifact=artifact, cleanup=cleanup, parameters=parameters)
      task_graph.add_task_template(task_template)
      for upstream_task_id in dependencies:
        task_graph.add_task_dependency(upstream_task_id, task_template.id)
    return task_graph
