import sys
import networkx
from tasks import TaskExecutionError


class Runner(object):
  def __init__(self, task_id, execution_graph):
    self.task_id = task_id
    self.execution_graph = execution_graph

  def check_artifacts(self):
    bfs_edges = networkx.bfs_edges(self.execution_graph.reverse(), self.task_id)
    reverse_execution_order = [self.task_id] + list(b for a,b in bfs_edges)
    for task_id in reverse_execution_order:

      for task in self.execution_graph.node[task_id]['tasks']:
        if task.artifact:
          if task.artifact.exists():
            print "Artifact exists", task.artifact.uri()
            task.status = "FOUND"
          else:
            task.status = "PENDING"
        else:
          task.status = "PENDING"
      pending = not all(t.status == "FOUND" for t in self.execution_graph.node[task_id]['tasks'])
      self.execution_graph.node[task_id]['pending'] = pending

  def run(self):
    self.check_artifacts()

    filter_pending_task_ids = lambda task_id: self.execution_graph.node[task_id]['pending']
    filter_not_pending_task_ids = lambda task_id: not self.execution_graph.node[task_id]['pending']
    
    pending_task_ids = filter(filter_pending_task_ids, self.execution_graph.nodes())
    pending_graph = self.execution_graph.subgraph(pending_task_ids)
    if self.task_id in pending_graph.nodes():
      bfs_tree = networkx.bfs_tree(pending_graph.reverse(), self.task_id)
      pending_execution_order = list(reversed([self.task_id] + list(b for a,b in bfs_tree.edges())))
    else:
      pending_execution_order = []

    for task_id in pending_execution_order:
      for task in self.execution_graph.node[task_id]['tasks']:
        task.status == "PENDING"
    
    for task_id in pending_execution_order:
      for task in self.execution_graph.node[task_id]['tasks']:
        print "Task {0} {1}".format(task_id, task.artifact.uri() if task.artifact else "")
        if task.command:
          print "Executing task", task_id
          print "executing command", task.command
          sys.stdout.flush()
          try:
            task.execute()
            task.status = "COMPLETED"
            print task.id, task.status
          except TaskExecutionError, e:
            print >>sys.stderr, "Error executing task:", task.id
            task.status = "ERROR"
            break
        else:
          task.status = "COMPLETED"

    for task_id in pending_execution_order:
      for task in self.execution_graph.node[task_id]['tasks']:
        if task.status == "PENDING":
          task.status = "ABORTED"

    for task_id in pending_execution_order:
      for task in self.execution_graph.node[task_id]['tasks']:
        pass #task.clean()

    bfs_tree = networkx.bfs_tree(self.execution_graph.reverse(), self.task_id)
    execution_order = list(b for a,b in bfs_tree.edges()) + [self.task_id]
    
    for task_id in execution_order:
      for task in self.execution_graph.node[task_id]['tasks']:
        print "\t".join([task.id, task.artifact.uri() if task.artifact else "", task.status])

    """
    if task.command:
      print task.command
      sys.stdout.flush()
      task.execute()
    """


