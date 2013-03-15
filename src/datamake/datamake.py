import sys, json
import artifacts
from tasks import TaskGraph, TaskTemplate, TaskExecutionError
from config import DatamakeConfig
import networkx

if __name__ == '__main__':
  config_filename = sys.argv[1]
  task_id = unicode(sys.argv[2])
  params = dict(param.split('=') for param in sys.argv[3:])

  dmconf = DatamakeConfig()
  dmconf.load_from_file(config_filename)
  dmconf.override_parameters = params
  task_graph = dmconf.task_graph()
  tasks = task_graph.resolve_subgraph(task_id, params)

  try:
    for task in tasks:
      if task.artifact:
        if task.artifact.exists():
          print task.artifact.uri(), "exists"
        else:
          print task.artifact.uri(), "does not exist"
          if task.command:
            print task.command
            sys.stdout.flush()
            task.execute()
  except TaskExecutionError, e:
    print >>sys.stderr, "Stopping at task:", task.id
    sys.exit(1)
  finally:
    for task in tasks:
      task.clean()

