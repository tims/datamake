import string
import artifacts
from graph import DirectedGraph
from tasks import Task

class TemplateKeyError(Exception):
  def __init__(self, template_string, key, parameters):
    self.template_string = template_string
    self.key = key
    self.parameters = parameters

  def __str__(self):
    return ("Missing key '{key}' for template string: {template_string}\n" +
      "Parameters: {parameters}").format(**self.__dict__)

class TaskTemplate:
  def __init__(self, **kvargs):
    self.namespace = kvargs['namespace']
    self.id = kvargs['id']
    self.command = kvargs['command']
    self.artifact = kvargs['artifact']
    self.verify = kvargs['verify']
    self.rollback = kvargs['rollback']
    self.cleanup = kvargs.get('cleanup', False)
    self.max_attempts = kvargs.get('max_attempts', 1)
    self.parameters = kvargs.get('parameters', {})
    self.dependencies = kvargs.get('dependencies', [])

  def _template(self, template_string, parameters):
    try:
      if isinstance(template_string, basestring) and template_string:
        return string.Template(template_string).substitute(parameters)
      else:
        return template_string
    except KeyError, e:
      raise TemplateKeyError(template_string, e.message, parameters)

  def resolve_task(self, template_parameters={}):
    params = dict(template_parameters)
    params.update(self.parameters)
    for k,v in params.items():
      params[k] = self._template(v, params)

    artifact = artifacts.resolve_artifact(self._template(self.artifact, params))
    command = self._template(self.command, params)
    qualified_task_id = ".".join((self.namespace, self.id))
    return Task(id=qualified_task_id, command=command, artifact=artifact, 
      cleanup=self.cleanup, max_attempts=self.max_attempts, template=self)

class TaskTemplateResolver():
  def __init__(self, task_templates=[]):
    self.template_graph = DirectedGraph()
    self.templates = {}
    self.template_parameters = {}
    for template in task_templates:
      self.add_task_template(template)

  def add_task_template(self, template):
    template_id = ".".join([template.namespace, template.id])
    print "Adding template with fq.id: %s" % template_id
    self.templates[template_id] = template
    self.template_parameters[template_id] = dict(template.parameters)
    self.template_graph.add_node(template_id)
    for task_id in template.dependencies:
      qualified_task_id = task_id if '.' in task_id else ".".join([template.namespace, task_id])
      print "adding edge %s to %s" % (qualified_task_id, template_id)
      self.template_graph.add_edge(qualified_task_id, template_id)

  def resolve_task_graph(self, template_id):
    if template_id not in self.templates:
      raise KeyError(template_id)

    reverse_graph = self.template_graph.reverse()
    nodes = reverse_graph.bfs_walk_graph(template_id)
    for node in nodes:
      print "node: %s" % node
      template = self.templates[node]
      for parent_node in reverse_graph[node]:
        print "parent_node: %s" % parent_node
        inherited_params = self.template_parameters[parent_node]
        params = self.template_parameters[node]
        inherited_params.update(params)

    task_graph = self.template_graph.subgraph(nodes)
    for node in nodes:
      template = self.templates[node]
      params = self.template_parameters[node]
      task = template.resolve_task(params)
      task_graph.node[node]['task'] = task
    return task_graph
