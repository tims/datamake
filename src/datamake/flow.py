import networkx as nx
import json
import datetime
from string import Template
from boto.s3.connection import S3Connection
import os
import re
import subprocess

def date_now():  
  dt = datetime.datetime.now()
  return dt.strftime("%Y-%m-%d")

def date_days_delta(datestring, days_delta):
  dt = datetime.datetime.strptime(datestring, "%Y-%m-%d")
  delta = datetime.timedelta(int(days_delta))
  return (dt + delta).strftime("%Y-%m-%d")

class Task:
  def __init__(self, id, command=None, artifact=None, dependencies=[], delete_after_use=False, **kvargs):
    self.id = id
    self.command = command
    self.artifact = artifact
    self.dependencies = dependencies
    self.parameters = kvargs
    self.delete_after_use = delete_after_use

class FlowManager:
  def __init__(self):
    self.tasks = {}
    self.task_graph = nx.DiGraph()

  def load_tasks(self, filename):
    f = open(filename)
    conf = json.load(f)
    for taskconf in conf:
      taskconf = dict((str(k),v) for k,v in taskconf.items())
      task = Task(**taskconf)
      self.tasks[task.id] = task
      self.task_graph.add_node(task.id)
      for dependency in task.dependencies:
        self.task_graph.add_edge(dependency['id'], task.id)

  def template(self, template_string, parameters):
    if template_string and parameters:
      return Template(template_string).substitute(parameters)
    else:
      return template_string

  def get_execution_order(self, id, flow):
    if id in flow:
      execution_order = [id] + list(b for a,b in nx.bfs_edges(flow.reverse(), id))
      execution_order.reverse()
    else:
      execution_order = []
    return execution_order

  def execute_flow(self, id, parameters):
    flow = self.build_flow(id, parameters=parameters)

    execution_order = get_execution_order(id, flow)
    
    delete_after_use_artifacts = []
    remove_tasks = []
    for taskid in execution_order:
      if taskid in flow:
        task = self.tasks[taskid]
        needs_execution_params = []
        print "Checking artifacts for task", taskid
        for task_parameters in flow.node[taskid].get('params', [{}]):
          if task.artifact is not None:
            artifact = resolve_artifact(self.template(task.artifact, task_parameters))
            if task.delete_after_use:
              delete_after_use_artifacts.append(artifact)
            exists = artifact.exists()
            print "Checking artifact: " + str(artifact.uri())
            print "found"  if exists else "not found"
          else:
            exists = False
          if not exists:
            needs_execution_params.append(task_parameters)
        if needs_execution_params:
          flow.node[taskid]['params'] = needs_execution_params
        else: 
          remove_tasks.append(taskid)
    
    print "remove tasks", remove_tasks
    for taskid in remove_tasks:
      if taskid in flow:
        subtree = list(nx.dfs_preorder_nodes(flow.reverse(), taskid))
        print "Trimming tasks from execution tree:", " ".join(map(str,subtree))
        flow.remove_nodes_from(subtree)

    execution_order = get_execution_order(id, flow)

    print "Starting task flow"
    for taskid in execution_order:
      task = self.tasks[taskid]
      for task_parameters in flow.node[taskid]['params']:
        self.execute_task(task.command, task_parameters)
      print "End task:", taskid

    for artifact in delete_after_use_artifacts:
      print "deleting", artifact.uri()

  def execute_task(self, command, parameters):
    print "executing"
    if command:
      command = Template(command).substitute(parameters)
      print command
      subprocess.check_call(command, shell=True)


  def build_flow(self, id, flow_graph=None, parameters={}):
    task = self.tasks[id]
    if not flow_graph:
      flow_graph = nx.DiGraph()

    task_parameters = dict(task.parameters)
    for k,v in task_parameters.items():
      task_parameters[k] = Template(v).substitute(parameters)
    task_parameters.update(dict(parameters))
    
    if task.command:
      command = Template(task.command).substitute(task_parameters)
    if task.artifact:
      artifact = Template(task.artifact).substitute(task_parameters)

    if id in flow_graph.node:
      params_set = flow_graph.node[id].get("params", [])
      params_set.append(task_parameters)
      flow_graph.node[id]["params"] = params_set
    else:
      flow_graph.add_node(id, data={"params":[task_parameters]})

    for dependency_parameters in task.dependencies:
      dependency_parameters = dict(dependency_parameters)
      dependency_id = dependency_parameters.pop('id')
      for params in self.resolve_dependency_parameters(dependency_parameters, task_parameters):
        flow_graph.add_node(dependency_id)
        flow_graph.add_edge(dependency_id, id)
        self.build_flow(dependency_id, flow_graph=flow_graph, parameters=params)
    return flow_graph

  def resolve_dependency_parameters(self, dependency_parameters, inherited_parameters):
    templated_params = {}
    for key, value in dependency_parameters.items():
      try:
        value = Template(value).substitute(inherited_parameters)
      except KeyError, e:
        print "Could not resolve template parameter", e
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
    for point in product(*templated_params.values()):
      params = dict(zip(templated_params.iterkeys(), point))
      for k,v in inherited_parameters.iteritems():
        if k not in params: params[k] = v
      yield params

# itertools.product for python 2.5
def product(*args, **kwds):
    # product('ABCD', 'xy') --> Ax Ay Bx By Cx Cy Dx Dy
    # product(range(2), repeat=3) --> 000 001 010 011 100 101 110 111
    pools = map(tuple, args) * kwds.get('repeat', 1)
    result = [[]]
    for pool in pools:
        result = [x+[y] for x in result for y in pool]
    for prod in result:
        yield tuple(prod)


class Artifact:
  def uri(self):
    raise Exception("not implemented")

  def exists(self):
    raise Exception("not implemented")

  def delete(self):
    raise Exception("not implemented")

class S3Artifact(Artifact):
  def __init__(self, uri):
    self.uri = uri
    pattern = re.compile('s3://(.+?)/(.+)')
    self.bucket, self.key = pattern.match(uri).groups()

  def uri(self):
    return self.uri

  def exists(self):
    from boto.s3.connection import S3Connection
    conn = S3Connection()
    b = conn.get_bucket(self.bucket)
    if b.get_key(self.key):
      return True
    else:
      return False

  def delete(self):
    from boto.s3.connection import S3Connection
    conn = S3Connection(self.access_id, self.private_key)
    b = conn.get_bucket(self.bucket)
    b.delete_key(self.key)

class HTTPArtifact(Artifact):
  def __init__(self, url):
    self.url = url

  def uri(self):
    return self.url

  def exists(self):
    r = requests.head(self.url)
    if r.status_code == 404:
      return False
    elif r.status_code == 200 or r.status_code == 302:
      return True
    else:
      raise Exception("Unexpected status code: %s" % r.status_code)

  def delete(self):
    r = requests.delete(self.url)

  def delete(self):
    command = 'ssh %s "rm %s"' % (self.host, self.path)
    os.system(command)

class FileArtifact(Artifact):
  def __init__(self, path):
    self.path = path

  def uri(self):
    return self.path

  def exists(self):
    if not self.path:
      raise Exception("invalid path " + self.path)
    return os.path.exists(self.path)

  def delete(self):
    command = 'rm %s' % self.path
    os.system(command)

def resolve_artifact(uri):
  if uri.startswith("http://"):
    return HTTPArtifact(uri)
  elif uri.startswith("s3://"):
    return S3Artifact(uri)
  else:
    return FileArtifact(uri)


if __name__ == "__main__":
  import sys
  flowman = FlowManager()
  flowman.load_tasks(sys.argv[1])
  id = sys.argv[2]
  params = dict(x.split("=") for x in sys.argv[3:])
  flowman.execute_flow(id, params)

