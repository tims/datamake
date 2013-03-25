import sys
from tasks import TaskGraph, TaskTemplate, TaskExecutionError
from config import DatamakeConfig
import runner


def main(args):
  config_filename = args[1]
  task_id = unicode(args[2])
  params = dict(param.split('=') for param in args[3:])

  dmconf = DatamakeConfig()
  dmconf.load_from_file(config_filename)
  dmconf.override_parameters = params
  task_graph = dmconf.task_graph()
  
  execution_graph = task_graph.resolve_execution_graph(task_id)
  runner.Runner(task_id, execution_graph).run()

if __name__ == '__main__':
  main(sys.argv)
