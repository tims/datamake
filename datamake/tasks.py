import sys
import json
import networkx
import itertools
import subprocess
from string import Template

import artifacts
from utils import *

class TaskGraph(object):
  def __init__(self):
    self.task_templates = {}
    self.tasks = {}
    self.graph = networkx.DiGraph()

  def add_task_template(self, task_template):
    self.task_templates[task_template.id] = task_template
    self.graph.add_node(task_template.id)

  def add_task(self, task):
    task_set = self.tasks.get(task.id, set())
    task_set.add(task)
    self.tasks[task.id] = task_set
    self.graph.add_node(task.id)

  def add_task_dependency(self, upstream_task_id, downstream_task_id):
    self.graph.add_edge(upstream_task_id, downstream_task_id)

  def resolve_execution_tasks(self, task_id):
    if not task_id in self.task_templates:
      raise TaskNotFoundError(task_id)

    reverse_execution_order = [task_id] + list(b for a,b in networkx.bfs_edges(self.graph.reverse(), task_id))
    subgraph = self.graph.subgraph(reverse_execution_order)
    for task_id in reverse_execution_order:
      task_template = self.task_templates[task_id]

      parameters = subgraph.node[task_id].get('parameters', {})
      parameters.update(task_template.parameters)
      subgraph.node[task_id]['parameters'] = parameters

      # get predecessors
      for next_task_id in subgraph.reverse()[task_id]:
        next_task_parameters = subgraph.node[next_task_id].get('parameters', {})
        next_task_parameters.update(parameters)
        subgraph.node[next_task_id]['parameters'] = next_task_parameters

    execution_order = reverse_execution_order
    execution_order.reverse()
    for task_id in execution_order:
      parameters = subgraph.node[task_id].get('parameters',{})
      for task in self.task_templates[task_id].tasks(parameters):
        self.add_task(task)
    execution_tasks = itertools.chain(*(sorted(self.tasks[task_id]) for task_id in execution_order))
    return execution_tasks

  def resolve_execution_graph(self, task_id):
    if not task_id in self.task_templates:
      raise TaskNotFoundError(task_id)

    reverse_execution_order = [task_id] + list(b for a,b in networkx.bfs_edges(self.graph.reverse(), task_id))
    subgraph = self.graph.subgraph(reverse_execution_order)
    for task_id in reverse_execution_order:
      task_template = self.task_templates[task_id]

      parameters = subgraph.node[task_id].get('parameters', {})
      parameters.update(task_template.parameters)
      subgraph.node[task_id]['parameters'] = parameters

      # get predecessors
      for next_task_id in subgraph.reverse()[task_id]:
        next_task_parameters = subgraph.node[next_task_id].get('parameters', {})
        next_task_parameters.update(parameters)
        subgraph.node[next_task_id]['parameters'] = next_task_parameters

    execution_order = reverse_execution_order
    execution_order.reverse()
    execution_graph = self.graph.subgraph(execution_order)
    for task_id in execution_order:
      parameters = subgraph.node[task_id].get('parameters',{})
      execution_graph.node[task_id]['tasks'] = list(self.task_templates[task_id].tasks(parameters))
    return execution_graph

  def dot(self, task_ids):
    g = self.graph.subgraph(task_ids)
    networkx.write_dot(g, filename)

class TaskTemplate:
  def __init__(self, **kvargs):
    self.id = kvargs['id']
    self.command = kvargs['command']
    self.artifact = kvargs['artifact']
    self.cleanup = kvargs.get('cleanup', False)
    self.max_attempts = kvargs.get('max_attempts', 1)
    self.parameters = kvargs.get('parameters', {})

  def _template(self, template_string, parameters):
    try:
      if template_string and parameters:
        return Template(template_string).substitute(parameters)
      else:
        return template_string
    except KeyError, e:
      raise TemplateKeyError(self.id, template_string, e.message)

  def tasks(self, inherited_parameters={}):
    params = dict(inherited_parameters)
    params.update(self.parameters)
    templated_params = {}

    # eval expression parameters
    for key, value in params.items()  :
      if isinstance(value, basestring):
        value = self._template(value, inherited_parameters)
        if not value.startswith("="):
          templated_params[key] = templated_params.get(key, []) + [value]
        else:
          eval_value = eval(value[1:])
          if isinstance(eval_value, basestring):
            templated_params[key] = templated_params.get(key, []) + [eval_value]
          elif hasattr(eval_value, "__iter__"):
            for v in eval_value:
              templated_params[key] = templated_params.get(key, []) + [v]
          else:
            templated_params[key] = templated_params.get(key, []) + [eval_value]
      else:
        templated_params[key] = templated_params.get(key, []) + [value]

    # expand multivalued params into multiple tasks
    for point in itertools.product(*templated_params.values()):
      params = dict(zip(templated_params.iterkeys(), point))
      for k,v in inherited_parameters.iteritems():
        if k not in params: params[k] = v

      # actually resolve the artifact, command etc
      artifact = artifacts.resolve_artifact(self._template(self.artifact, params)) if self.artifact else None
      command = self._template(self.command, params)
      yield Task(id=self.id, command=command, artifact=artifact, cleanup=self.cleanup, max_attempts=self.max_attempts)

class TaskNotFoundError(Exception):
  def __init__(self, id):
    self.id = id

  def __str__(self):
    return repr(id)

class TemplateKeyError(Exception):
  def __init__(self, task_id, template_string, key):
    self.task_id = task_id
    self.template_string = template_string
    self.key = key

  def __str__(self):
    return repr(self.__dict__)

class TaskExecutionError(Exception):
  def __init__(self, task):
    self.task = task

  def __str__(self):
    return repr(self.task.__dict__)


class Task:
  def __init__(self, **kvargs):
    self.id = kvargs['id']
    self.pre_command = None
    self.post_command = None
    self.command = kvargs.get('command', None)
    self.artifact = kvargs.get('artifact', None)
    self.cleanup = kvargs.get('cleanup', False)
    self.max_attempts = kvargs.get('max_attempts', 1)

  def _run_command(self):
    try:
      command = self.command
      if self.pre_command:
        command = "{0} && {1}".format(self.pre_command, command)
      if self.post_command:
        command = "{0} && {1}".format(command, self.post_command)
      print self.command
      subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError:
      raise TaskExecutionError(self)
    finally:
      sys.stdout.flush()
      sys.stderr.flush()

  def execute(self):
    attempts = 0
    while True:
      try:
        if self.command:
          attempts += 1
          self._run_command()
          return
      except TaskExecutionError:
        print >>sys.stderr, "attempt {0} failed".format(attempts)
        if attempts >= self.max_attempts:
          print >>sys.stderr, "max attempts reached"
          raise

  def clean(self):
    if self.artifact:
      if self.cleanup:
        if self.artifact.exists():
          print "cleaning up artifact", self.artifact.uri()
          self.artifact.delete()

  def tuple(self): return (self.id, self.command, self.artifact.uri() if self.artifact else None, self.cleanup, self.max_attempts)
  def __eq__(self, other): return self.tuple() == other.tuple()
  def __ne__(self, other): return self.tuple() != other.tuple()
  def __lt__(self, other): return self.tuple() < other.tuple()
  def __le__(self, other): return self.tuple() <= other.tuple()
  def __gt__(self, other): return self.tuple() > other.tuple()
  def __ge__(self, other): return self.tuple() >= other.tuple()
  def __hash__(self): return hash(self.tuple())



