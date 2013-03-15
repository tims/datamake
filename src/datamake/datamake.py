import sys, json
import artifacts
from task import TaskGraph, TaskTemplate
from config import DatamakeConfig

if __name__ == '__main__':
  config_filename = sys.argv[1]
  task_id = sys.argv[2]
  params = dict(param.split('=') for param in sys.argv[3:])

  dmconf = DatamakeConfig()
  dmconf.load_from_file(config_filename)
  dmconf.override_parameters = params
  task_graph = dmconf.task_graph()
  tasks = task_graph.resolve_subgraph(task_id, params)

  for task in tasks:
    print json.dumps(task.__dict__, indent=True, default=lambda x: x.uri() if isinstance(x, artifacts.Artifact) else x.__dict__)
    print "Executing task", task.id
    task.execute()

  

