import sys
from tasks import TaskGraph, TaskTemplate, TaskExecutionError
from config import DatamakeConfig


def main(args):
  config_filename = args[1]
  task_id = unicode(args[2])
  params = dict(param.split('=') for param in args[3:])

  dmconf = DatamakeConfig()
  dmconf.load_from_file(config_filename)
  dmconf.override_parameters = params
  task_graph = dmconf.task_graph()
  tasks = task_graph.resolve_execution_tasks(task_id)

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

if __name__ == '__main__':
  main(sys.argv)
