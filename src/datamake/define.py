import sys, json
import artifacts
from task import TaskGraph, TaskTemplate

class DatamakeConfig(object):
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

      task_template = TaskTemplate(id=id, command=command, artifact=artifact, cleanup=cleanup, parameters=parameters)
      task_graph.add_task_template(task_template)
      for upstream_task_id in dependencies:
        task_graph.add_task_dependency(upstream_task_id, task_template.id)
    return task_graph

if __name__ == '__main__':
  config_filename = sys.argv[1]
  task_id = sys.argv[2]
  params = dict(param.split('=') for param in sys.argv[3:])

  dmconf = DatamakeConfig()
  dmconf.load(config_filename)
  task_graph = dmconf.task_graph()
  tasks = task_graph.resolve_subgraph(task_id, params)

  for task in tasks:
    print json.dumps(task.__dict__, indent=True, default=lambda x: x.uri() if isinstance(x, artifacts.Artifact) else x.__dict__)
  task_graph.draw_graph("graph.png")

