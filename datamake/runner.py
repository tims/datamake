import sys
import networkx
from tasks import TaskExecutionError


class Runner(object):
  def __init__(self, task_id, task_graph):
    self.task_id = task_id
    self.task_graph = task_graph

  def show_graph(self):
    for task_id in self.task_graph.nodes():
      task = self.get_task(task_id)
      for dep in task.template.dependencies:
        print " %s: %s => %s" % (task.template.namespace, task.template.id, dep)
    return 0

  def get_execution_order(self, task_id, graph):
    if task_id in graph:
      return list(reversed(graph.reverse().bfs_walk_graph(task_id)))
    else:
      return []

  def get_task(self, task_id):
    return self.task_graph.node[task_id]['task']

  def get_pending_graph(self):
    """Get a subgraph of tasks that are pending starting from the target node"""
    pending_filter = lambda task_id: self.get_task(task_id).status == "PENDING"
    all_pending_tasks = filter(pending_filter, self.task_graph.nodes())
    pending_graph = self.task_graph.subgraph(all_pending_tasks) # subgraph of all pending nodes
    pending_tasks = self.get_execution_order(self.task_id, pending_graph) # get all accessible pending nodes
    pending_graph = self.task_graph.subgraph(pending_tasks) # subgraph of accessible pending nodes

    for task_id in (t for t in all_pending_tasks if t not in pending_tasks):
      self.get_task(task_id).status = "NOT_NEEDED"
    return pending_graph

  def check_artifacts(self):
    print "Checking artifacts"
    
    execution_order = self.get_execution_order(self.task_id, self.task_graph)
    for task_id in execution_order:
      task = self.task_graph.node[task_id]['task']
      if task.artifact:
        if task.artifact.exists():
          task.status = "FOUND"
        else:
          task.status = "PENDING"
      else:
        task.status = "PENDING"

  def get_pending_execution_order(self):
    self.check_artifacts()

    pending_graph = self.get_pending_graph()
    return self.get_execution_order(self.task_id, pending_graph)

  def print_all_task_status(self):
    execution_order = self.get_execution_order(self.task_id, self.task_graph)
    self.print_task_status(execution_order)

  def print_task_status(self, task_ids):
    for task_id in task_ids:
      task = self.get_task(task_id)
      print "\t".join([task.id, task.status, task.artifact.uri() if task.artifact else ""])

  def run_task(self, task_id):
    print "Task {0} starting".format(task_id)
    task = self.get_task(task_id)
    
    if task.artifact:
      print "Task artifact {0}".format(task.artifact.uri())
    if task.command:
      print "Executing task", task_id
      print "executing command", task.command
      sys.stdout.flush()
      
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
    for task_id in self.task_graph.nodes():
      task = self.get_task(task_id)
      if task.status == "PENDING":
        task.status = "ABORTED"

  def delete_artifacts(self, task_ids, force=False):
    for task_id in task_ids:
      task = self.get_task(task_id)
      task.delete_artifact(force=force)

  def delete_all_artifacts(self, force=False):
    task_ids = self.get_execution_order(self.task_id, self.task_graph)
    self.delete_artifacts(task_ids, force=force)


