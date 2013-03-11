import sys, json
import artifacts
from task import TaskGraph, TaskTemplate

class DatamakeConfig(object):
  def __init__(self, filename):
    self.filename = filename

  def load(self):
    self.config = json.load(open(self.filename))

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
  dmconf = DatamakeConfig(sys.argv[1])
  dmconf.load()
  task_graph = dmconf.task_graph()
  
  params = dict(param.split('=') for param in sys.argv[2:])
  tasks = task_graph.resolve_subgraph('deploy-science', params)
  for task in tasks:
    print json.dumps(task.__dict__, indent=True, default=lambda x: x.uri() if isinstance(x, artifacts.Artifact) else x.__dict__)
  task_graph.draw_graph("graph.png")