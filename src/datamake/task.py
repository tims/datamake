import sys
import json
import networkx
import itertools
import artifacts
from string import Template
from utils import *

class TaskGraph:
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

  def resolve_subgraph(self, task_id, parameters={}):
    reverse_execution_order = [task_id] + list(b for a,b in networkx.bfs_edges(self.graph.reverse(), task_id))
    inherited_parameters = parameters
    for current_task_id in reverse_execution_order:
      task_template = self.task_templates[current_task_id]
      for task in task_template.tasks(inherited_parameters):
        self.add_task(task) 
        inherited_parameters.update(task_template.parameters)

    execution_order = reverse_execution_order
    execution_order.reverse()
    tasks = itertools.chain(*(sorted(self.tasks[task_id]) for task_id in execution_order))
    return tasks

  def draw_graph(self, filename):
    import matplotlib.pyplot
    networkx.draw(self.graph)
    matplotlib.pyplot.savefig(filename)

class TaskTemplate:
  def __init__(self, **kvargs):
    self.id = kvargs['id']
    self.command = kvargs['command']
    self.artifact = kvargs['artifact']
    self.cleanup = kvargs.get('cleanup', False)
    self.parameters = kvargs.get('parameters', {})

  def _template(self, template_string, parameters):
    if template_string and parameters:
      return Template(template_string).substitute(parameters)
    else:
      return template_string

  def tasks(self, inherited_parameters={}):
    params = inherited_parameters
    params.update(self.parameters)

    templated_params = {}
    for key, value in params.items()  :
      try:
        value = self._template(value, inherited_parameters)
      except KeyError, e:
        print >>sys.stderr, "Could not resolve template parameter", key, value
        raise
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
          raise "Unable to handle evalutation of parameter, must be string or iterable: %s = %s" % (key, eval_value)

    for point in itertools.product(*templated_params.values()):
      params = dict(zip(templated_params.iterkeys(), point))
      for k,v in inherited_parameters.iteritems():
        if k not in params: params[k] = v

      artifact = artifacts.resolve_artifact(self._template(self.artifact, params)) if self.artifact else None
      command = self._template(self.command, params)
      yield Task(id=self.id, command=command, artifact=artifact, cleanup=self.cleanup)

class Task:
  def __init__(self, **kvargs):
    self.id = kvargs['id']
    self.command = kvargs['command']
    self.artifact = kvargs['artifact']
    self.cleanup = kvargs.get('cleanup', False)

  def tuple(self): return (self.id, self.command, self.artifact, self.cleanup)
  def __eq__(self, other): return self.tuple() == other.tuple()
  def __ne__(self, other): return self.tuple() != other.tuple()
  def __lt__(self, other): return self.tuple() < other.tuple()
  def __le__(self, other): return self.tuple() <= other.tuple()
  def __gt__(self, other): return self.tuple() > other.tuple()
  def __ge__(self, other): return self.tuple() >= other.tuple()
  def __hash__(self): return hash(self.tuple())



