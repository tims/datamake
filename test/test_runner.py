import unittest
import json
import datamake.tasks
import datamake.config
import datamake.artifacts
import datamake.runner
import networkx

class DatamakeConfigTest(unittest.TestCase):
  def tmpTask(self, task_id, artifact_exists=False):
    artifact = datamake.artifacts.TestArtifact(artifact_exists)
    return datamake.tasks.Task(id=task_id, artifact=artifact)

  def testExecutionOrder(self):
    graph = networkx.DiGraph()

    graph.add_node('task1', tasks=[self.tmpTask('task1')])
    graph.add_node('task2', tasks=[self.tmpTask('task2')])
    graph.add_edge('task1','task2')

    runner = datamake.runner.Runner('task2', graph)
    self.assertEquals(['task1','task2'], runner.get_execution_order())

    runner.check_artifacts()
    pending_graph = runner.get_pending_graph()
    self.assertEquals(['task1','task2'], runner.get_execution_order(pending_graph))

  def testExecutionOrderWithOnlyOnePendingTask(self):
    graph = networkx.DiGraph()
    graph.add_node('task1', tasks=[self.tmpTask('task1', True)])
    graph.add_node('task2', tasks=[self.tmpTask('task2')])
    graph.add_edge('task1','task2')

    runner = datamake.runner.Runner('task2', graph)
    self.assertEquals(['task1','task2'], runner.get_execution_order())

    runner.check_artifacts()
    pending_graph = runner.get_pending_graph()
    self.assertEquals(['task2'], runner.get_execution_order(pending_graph))

  def testExecutionOrderWithNonPendingBranch(self):
    graph = networkx.DiGraph()
    graph.add_node('task1', tasks=[self.tmpTask('task1')])
    graph.add_node('task2', tasks=[self.tmpTask('task2')])
    graph.add_node('task3', tasks=[self.tmpTask('task3', True)])
    graph.add_node('task4', tasks=[self.tmpTask('task4')])
    graph.add_edge('task1','task2')
    graph.add_edge('task1','task3')
    graph.add_edge('task2','task4')
    graph.add_edge('task3','task4')

    runner = datamake.runner.Runner('task4', graph)
    self.assertEquals(['task1','task2','task3','task4'], runner.get_execution_order())

    runner.check_artifacts()
    pending_graph = runner.get_pending_graph()
    self.assertEquals(['task1','task2','task4'], runner.get_execution_order(pending_graph))

