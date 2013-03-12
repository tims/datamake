import unittest
from StringIO import StringIO
from datamake import define

class DatamakeConfigTest(unittest.TestCase):
  def testLoad(self):
    dmconfig = define.DatamakeConfig()
    dmconfig.load(StringIO("""[
      {
        "id": "task1"
      }  
    ]
    """))

  def testTaskGraph(self):
    dmconfig = define.DatamakeConfig()
    dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo hello world",
        "artifact": "http://localhost:9090/bla/bla",
        "parameters": {
          "x": 1
        }
      }  
    ]
    """))
    task_graph = dmconfig.task_graph()
    task1 = task_graph.task_templates['task1']
    self.assertEqual(task1.id, 'task1', 'incorrect id')
    self.assertEqual(task1.command, 'echo hello world', 'incorrect command')
    self.assertEqual(task1.artifact, 'http://localhost:9090/bla/bla', 'incorrect artifact')
    self.assertEqual(task1.parameters['x'], 1, 'incorrect parameter')

if __name__ == "__main__":
    unittest.main()