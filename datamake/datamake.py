import sys
from tasks import TaskExecutionError
from templates import TaskTemplateResolver, TemplateKeyError
from config import DatamakeConfig
import runner
import json
import utils

def parse_args(args):
  try:
    import argparse
    return parse_args_with_argparse(args)
  except ImportError:
    import optparse
    return parse_args_with_optparse(args)

def parse_args_with_argparse(args):
  import argparse
  parser = argparse.ArgumentParser(description='Run datamake task flow.')
  parser.add_argument('task_id', metavar='task_id', type=str, help='target task to be run')
  parser.add_argument('config_files', metavar='config_file', type=str, nargs='+',
    help='task config files')
  parser.add_argument('--param', dest='parameters', action='append',
                     help='specify KEY=VALUE parameter that will override parameters on all tasks')
  parser.add_argument('--eval-param', dest='eval_parameters', action='append',
                     help='specify KEY=VALUE parameter that will override parameters on all tasks. VALUE will be replaced by eval(VALUE) in python. If the eval output is a list, the task flow will be executed per value.')
  parser.add_argument('--dryrun', dest='dryrun', action='store_true',
                     help='print all tasks and if they are pending but do not execute them')
  parser.add_argument('--delete-artifacts', dest='delete_artifacts', action='store_true',
                     help='beware! deletes all artifacts in the flow!')
  return parser.parse_args(args)

def parse_args_with_optparse(args):
  import optparse
  usage = """usage: %prog [-h] [--param PARAMETERS] [--eval-param EVAL_PARAMETERS]
                   [--dryrun] [--delete-artifacts]
                   task_id config_file [config_file ...]"""
  parser = optparse.OptionParser(usage=usage)
  parser.add_option('--param', dest='parameters', action='append',
                     help='specify KEY=VALUE parameter that will override parameters on all tasks')
  parser.add_option('--eval-param', dest='eval_parameters', action='append',
                     help='specify KEY=VALUE parameter that will override parameters on all tasks. VALUE will be replaced by eval(VALUE) in python. If the eval output is a list, the task flow will be executed per value.')
  parser.add_option('--dryrun', dest='dryrun', action='store_true',
                     help='print all tasks and if they are pending but do not execute them')
  parser.add_option('--delete-artifacts', dest='delete_artifacts', action='store_true',
                     help='beware! deletes all artifacts in the flow!')

  (options, remaining) = parser.parse_args()
  if len(remaining) < 2: 
    print "Not enough arguments, need: task_id config_files | [config_file]"
  options.task_id = remaining[0]
  options.config_files = remaining[1:]
  return options


def run_tasks(task_runner, pending_tasks):
  try:
    for task_id in pending_tasks:
      task_runner.run_task(task_id)
    return 0 
  except TaskExecutionError, e:
    print "Error while executing task ", task_id
    print e.task.tuple()
    print e.message
    task_runner.abort_pending_tasks()
    return 1
  finally:
    task_runner.delete_artifacts(pending_tasks)

def dry_run_tasks(task_runner, pending_tasks):
  print "Starting dry run"
  for task_id in pending_tasks:    
    task = task_runner.get_task(task_id)
    if task.command:
      print "command:", task.command
      if task.artifact:
        print "artifact:",task.artifact.uri()
  print "Dry run complete"
  return 0

def get_config(config_filename):
  config = DatamakeConfig()
  config.load_from_file(config_filename)
  return config

def get_template_resolver(config, override_parameters={}):
  task_templates = config.task_templates(override_parameters)
  template_resolver = TaskTemplateResolver(task_templates)
  return template_resolver

def main():
  args = parse_args(sys.argv[1:])

  config_filename = args.config_files[0]
  task_id = args.task_id

  parameters = dict(param.split('=') for param in args.parameters) if args.parameters else {}
  override_parameters_list = []
  if args.eval_parameters:
    evaled_parameters_list = list(utils.evalulate_parameters(dict(param.split('=') for param in args.eval_parameters)))
    for evaled_parameters in evaled_parameters_list:
      override_parameters = dict(evaled_parameters)
      override_parameters.update(parameters)
      override_parameters_list.append(override_parameters)
  else:
    override_parameters_list = [parameters]

  config = get_config(config_filename)

  exit_status = 0
  for override_parameters in override_parameters_list:
    print "Starting Flow"
    print "Override params: %s" % json.dumps(override_parameters, indent=True)
    template_resolver = get_template_resolver(config, override_parameters)
    try:
      task_graph = template_resolver.resolve_task_graph(task_id)
    except TemplateKeyError, e:
      print e
      exit_status = 1
      break

    task_runner = runner.Runner(task_id, task_graph)
    pending_tasks = task_runner.get_pending_execution_order()

    print "Task status:"
    task_runner.print_all_task_status()
    print "Trimming tasks..."
    print "Pending tasks"
    task_runner.print_task_status(pending_tasks)
    if args.dryrun:
      exit_status += dry_run_tasks(task_runner, pending_tasks)
    elif args.delete_artifacts:
      print "Forcing removal of existing artifacts"
      task_runner.delete_all_artifacts(force=True)
    else:
      exit_status += run_tasks(task_runner, pending_tasks)
      print "Final status"
      task_runner.print_all_task_status()
    print
    if exit_status:
      break
  print "FAILED" if exit_status else "SUCCESS"
  return exit_status

if __name__ == '__main__':
  main()
