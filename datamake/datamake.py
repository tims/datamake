import sys
from tasks import TaskGraph, TaskTemplate, TaskExecutionError
from config import DatamakeConfig
import runner

import argparse

def parse_args():
  parser = argparse.ArgumentParser(description='Run datamake task flow.')
  parser.add_argument('task_id', metavar='task_id', type=str, help='target task to be run')
  parser.add_argument('config_files', metavar='config_file', type=str, nargs='+',
    help='task config files')
  parser.add_argument('--param', dest='parameters', action='append',
                     help='specify KEY=VALUE parameter parameter that will override parameters on all tasks')
  parser.add_argument('--dryrun', dest='dryrun', action='store_true',
                     help='print pending tasks but do not execute them')
  parser.add_argument('--force', dest='force', action='store_true',
                     help='force cleanup of all cleanable tasks before running')
  return parser.parse_args()

def main():
  args = parse_args()

  config_filename = args.config_files[0]
  task_id = args.task_id
  params = dict(param.split('=') for param in args.parameters) if args.parameters else {}

  dmconf = DatamakeConfig()
  dmconf.load_from_file(config_filename)
  dmconf.override_parameters = params
  task_graph = dmconf.task_graph()
  
  execution_graph = task_graph.resolve_execution_graph(task_id)
  task_runner = runner.Runner(task_id, execution_graph)

  if args.force:
    task_runner.clean_all_tasks()

  execution_order = task_runner.get_pending_execution_order()

  print
  print "Tasks to be run:", " ".join(execution_order)

  try:
    for task_id in execution_order:
      task_runner.run_task(task_id)
    exit_status = 0 
  except TaskExecutionError, e:
    print "Error while executing task ", task_id
    print e.task.tuple()
    print e.message
    exit_status = 1
  finally:
    print
    print "Final status"
    task_runner.abort_pending_tasks()
    task_runner.print_all_task_status()
    task_runner.clean_tasks(execution_order)
  
  print "FAILED" if exit_status else "SUCCESS"
  return exit_status

if __name__ == '__main__':
  main()
