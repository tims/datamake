import networkx as nx
import json
from string import Template

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

class DataMaker:
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


  def make(self, id, parameters):
    flow = self.build_flow(id, parameters=parameters)
    execution_tree = nx.DiGraph()
    for a,b in nx.bfs_edges(flow.reverse(), id):
      print a,b
      execution_tree.add_edge(a,b)


    print execution_tree.edges()
    execution_order = [id] + list(b for a,b in nx.bfs_edges(flow.reverse(), id))
    execution_order.reverse()
    print 'Execution_order:'
    for task in execution_order:
      print "\t",task

    print "Starting task flow"
    for taskid in execution_order:
      print
      print "Begin task:", taskid
      if 'params' in flow.node[taskid]:
        for task_parameters in flow.node[taskid]['params']:
          self.execute_task(taskid, task_parameters)
      else:
        self.execute_task(taskid, {})
      print "End task:", taskid

  def execute_task(self, id, parameters):
    task = self.tasks[id]
    
    if task.artifact is not None:
      artifact = Template(task.artifact).substitute(parameters)

    if not task.command: return

    command = Template(task.command).substitute(parameters)
    
    print "Parameters:", json.dumps(parameters)
    print "Command:", command

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

if __name__ == "__main__":
  import sys
  maker = DataMaker()
  maker.load_tasks(sys.argv[1])
  id = sys.argv[2]
  params = dict(x.split("=") for x in sys.argv[3:])
  maker.make(id, params)

