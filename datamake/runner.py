import sys
import networkx
from tasks import TaskExecutionError


class Runner(object):
  def __init__(self, task_id, execution_graph):
    self.task_id = task_id
    self.execution_graph = execution_graph

  def get_execution_order(self, graph=None):
    if graph is None:
      graph = self.execution_graph
    if self.task_id in graph.nodes():
      if len(graph.nodes()) == 1:
        return [self.task_id]
      else:
        edges = networkx.bfs_edges(graph.reverse(), self.task_id)
        execution_order = [self.task_id] + list(b for a,b in edges)
        execution_order.reverse()
        print list(execution_order)
    else:
      execution_order = []
    return execution_order

  def get_pending_graph(self):
    filter_pending_task_ids = lambda task_id: self.execution_graph.node[task_id]['pending']
    pending_task_ids = filter(filter_pending_task_ids, self.execution_graph.nodes())
    pending_graph = self.execution_graph.subgraph(pending_task_ids) # limit graph to all pending nodes
    pending_execution_order = self.get_execution_order(pending_graph) # get all accessible nodes
    pending_graph = self.execution_graph.subgraph(pending_execution_order) # limit to accessible nodes 
    return pending_graph

  def check_artifacts(self):
    print "Checking artifacts"
    
    execution_order = self.get_execution_order()
    for task_id in reversed(execution_order):

      for task in self.execution_graph.node[task_id]['tasks']:
        if task.artifact:
          if task.artifact.exists():
            task.status = "FOUND"
          else:
            task.status = "PENDING"
        else:
          task.status = "PENDING"
      pending = not all(t.status == "FOUND" for t in self.execution_graph.node[task_id]['tasks'])
      self.execution_graph.node[task_id]['pending'] = pending

    print "Task status"
    self.print_task_status(execution_order)

  def get_pending_execution_order(self):
    self.check_artifacts()

    filter_pending_task_ids = lambda task_id: self.execution_graph.node[task_id]['pending']
    pending_task_ids = filter(filter_pending_task_ids, self.execution_graph.nodes())
    pending_graph = self.execution_graph.subgraph(pending_task_ids)
    if self.task_id in pending_graph.nodes():
      bfs_tree = networkx.bfs_tree(pending_graph.reverse(), self.task_id)
      pending_execution_order = list(reversed(list(b for a,b in bfs_tree.edges()) + [self.task_id]))
    else:
      pending_execution_order = []

    for task_id in pending_execution_order:
      for task in self.execution_graph.node[task_id]['tasks']:
        task.status == "PENDING"

    print
    print "Pending task status"
    self.print_task_status(pending_execution_order)
    return pending_execution_order
    
  def print_all_task_status(self):
    execution_order = self.get_execution_order()
    self.print_task_status(execution_order)

  def print_task_status(self, task_ids):
    for task_id in task_ids:
      for task in self.execution_graph.node[task_id]['tasks']:
        print "\t".join([task.id, task.status, task.artifact.uri() if task.artifact else ""])

  def run_task(self, task_id):
    print "Task {0} starting".format(task_id)
    for task in self.execution_graph.node[task_id]['tasks']:
      if task.artifact:
        print "Task artifact {0}".format(task.artifact.uri())
      if task.command:
        print "Executing task", task_id
        print "executing command", task.command
        sys.stdout.flush()
        if task.status == "PENDING":
          print "Executing Task {0} {1}".format(task_id, task.artifact.uri() if task.artifact else "")
          try:
            task.execute()
          except TaskExecutionError:
            task.status = "ERROR"
            raise
          task.status = "COMPLETED"
          print task.id, task.status
      else:
        print "Task {0} complete".format(task.id)
        task.status = "COMPLETED"

  def abort_pending_tasks(self):
    for task_id in self.execution_graph.nodes():
      for task in self.execution_graph.node[task_id]['tasks']:
        if task.status == "PENDING":
          task.status = "ABORTED"

  def clean_all_tasks(self):
    execution_order = self.get_execution_order()
    self.clean_tasks(execution_order)

  def clean_tasks(self, task_ids):
    print 
    for task_id in task_ids:
      for task in self.execution_graph.node[task_id]['tasks']:
        task.clean()

