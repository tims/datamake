import unittest
import json
from StringIO import StringIO
import datamake.tasks
import datamake.config
import datamake.artifacts

class DatamakeConfigTest(unittest.TestCase):
  def setUp(self):
    self.dmconfig = datamake.config.DatamakeConfig()

  def testLoad(self):
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1"
      }  
    ]
    """))
    task_graph = self.dmconfig.task_graph()
    template_task = task_graph.task_templates['task1']
    self.assertEqual(template_task.id, 'task1', 'incorrect id')
    self.assertEqual(template_task.command, None, 'incorrect command')
    self.assertEqual(template_task.artifact, None, 'incorrect artifact')
    self.assertEqual(template_task.parameters, {}, 'incorrect parameter')


  def testSingleTask(self):
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo hello world",
        "artifact": "/tmp/foo",
        "parameters": {
          "x": 1
        }
      }  
    ]
    """))
    task_graph = self.dmconfig.task_graph()
    template_task = task_graph.task_templates[u'task1']
    self.assertEqual(template_task.id, 'task1', 'incorrect id')
    self.assertEqual(template_task.command, 'echo hello world', 'incorrect command')
    self.assertEqual(template_task.artifact, '/tmp/foo', 'incorrect artifact')
    self.assertEqual(template_task.parameters, {'x': 1}, 'incorrect parameter')

    tasks = list(task_graph.resolve_subgraph('task1'))
    self.assertEqual(len(tasks), 1)
    task = tasks[0]

    self.assertEqual(task.id, 'task1', 'incorrect id')
    self.assertEqual(task.command, 'echo hello world', 'incorrect command')
    self.assertEqual(task.artifact.uri(), '/tmp/foo', 'incorrect artifact')

  def testSingleParameterizedTask(self):
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo ${message}",
        "artifact": "/tmp/${filename}",
        "parameters": {
          "message": "hello world",
          "filename": "foo"
        }
      }  
    ]
    """))
    task_graph = self.dmconfig.task_graph()
    template_task = task_graph.task_templates['task1']
    self.assertEqual(template_task.id, 'task1', 'incorrect id')
    self.assertEqual(template_task.command, 'echo ${message}', 'incorrect command')
    self.assertEqual(template_task.artifact, '/tmp/${filename}', 'incorrect artifact')
    self.assertEqual(template_task.parameters, {'message':'hello world','filename':'foo'}, 'incorrect parameter')

    tasks = list(task_graph.resolve_subgraph('task1'))
    self.assertEqual(len(tasks), 1)
    task = tasks[0]

    self.assertEqual(task.id, 'task1', 'incorrect id')
    self.assertEqual(task.command, 'echo hello world', 'incorrect command')
    self.assertEqual(task.artifact.uri(), '/tmp/foo', 'incorrect artifact')

  def testUpstreamTaskInheritsDownstreamParams(self):
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo ${message}",
        "artifact": "/tmp/${filename}"
      },
      {
        "id": "task2",
        "command": "echo goodbye",
        "artifact": "/tmp/bar",
        "dependencies": ["task1"],
        "parameters": {
          "message": "hello",
          "filename": "foo"
        }
      }  
    ]
    """))
    task_graph = self.dmconfig.task_graph()
    task1, task2 = task_graph.resolve_subgraph('task2')
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2', command='echo goodbye', artifact=datamake.artifacts.FileArtifact('/tmp/bar')))

  def testUpstreamParamsOverrideDownstreamParams(self):
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo ${message}",
        "artifact": "/tmp/${filename}",
        "parameters": {
          "message": "hello",
          "filename": "foo"
        }
      },
      {
        "id": "task2",
        "command": "echo ${message}",
        "artifact": "/tmp/${filename}",
        "dependencies": ["task1"],
        "parameters": {
          "message": "goodbye",
          "filename": "bar"
        }
      }  
    ]
    """))
    task_graph = self.dmconfig.task_graph()
    task1, task2 = task_graph.resolve_subgraph('task2')
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2', command='echo goodbye', artifact=datamake.artifacts.FileArtifact('/tmp/bar')))

  def testEvalParams(self):
    dmconfig = datamake.config.DatamakeConfig()
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo hello ${count}",
        "artifact": "/tmp/foo${count}",
        "parameters": {
          "count": "=1 + 1"
        }
      }
    ]
    """))
    task_graph = self.dmconfig.task_graph()
    task1, = task_graph.resolve_subgraph('task1')
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello 2', artifact=datamake.artifacts.FileArtifact('/tmp/foo2')))

  def testEvalParamsToManyTasks(self):
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo hello ${count}",
        "artifact": "/tmp/foo${count}",
        "parameters": {
          "count": "=[1,2,3]"
        }
      }
    ]
    """))
    task_graph = self.dmconfig.task_graph()
    t1,t2,t3 = task_graph.resolve_subgraph('task1')
    self.assertEqual(t1, datamake.tasks.Task(id='task1', command='echo hello 1', artifact=datamake.artifacts.FileArtifact('/tmp/foo1')))
    self.assertEqual(t2, datamake.tasks.Task(id='task1', command='echo hello 2', artifact=datamake.artifacts.FileArtifact('/tmp/foo2')))
    self.assertEqual(t3, datamake.tasks.Task(id='task1', command='echo hello 3', artifact=datamake.artifacts.FileArtifact('/tmp/foo3')))

  def testEvalParamsToManyDependentTasks(self):
    dmconfig = datamake.config.DatamakeConfig()
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo hello ${count}",
        "artifact": "/tmp/foo${count}"
      },
      {
        "id": "task2",
        "command": "echo goodbye",
        "artifact": "/tmp/bar",
        "dependencies": ["task1"],
        "parameters": {
          "count": "=[1,2,3]"
        }
      } 
    ]
    """))
    task_graph = self.dmconfig.task_graph()
    for t in task_graph.resolve_subgraph('task2'):
      print t.__dict__
    t11,t12,t13,t2 = task_graph.resolve_subgraph('task2')
    self.assertEqual(t11, datamake.tasks.Task(id='task1', command='echo hello 1', artifact=datamake.artifacts.FileArtifact('/tmp/foo1')))
    self.assertEqual(t12, datamake.tasks.Task(id='task1', command='echo hello 2', artifact=datamake.artifacts.FileArtifact('/tmp/foo2')))
    self.assertEqual(t13, datamake.tasks.Task(id='task1', command='echo hello 3', artifact=datamake.artifacts.FileArtifact('/tmp/foo3')))
    self.assertEqual(t2, datamake.tasks.Task(id='task2', command='echo goodbye', artifact=datamake.artifacts.FileArtifact('/tmp/bar')))

  def testEvalParamsToManyTasksWithSingleDependentTask(self):
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo hello",
        "artifact": "/tmp/foo"
      },
      {
        "id": "task2",
        "command": "echo goodbye ${count}",
        "artifact": "/tmp/bar${count}",
        "dependencies": ["task1"],
        "parameters": {
          "count": "=[1,2,3]"
        }
      } 
    ]
    """))
    t1,t21,t22,t23 = self.dmconfig.task_graph().resolve_subgraph('task2')
    self.assertEqual(t1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo')))
    self.assertEqual(t21, datamake.tasks.Task(id='task2', command='echo goodbye 1', artifact=datamake.artifacts.FileArtifact('/tmp/bar1')))
    self.assertEqual(t22, datamake.tasks.Task(id='task2', command='echo goodbye 2', artifact=datamake.artifacts.FileArtifact('/tmp/bar2')))
    self.assertEqual(t23, datamake.tasks.Task(id='task2', command='echo goodbye 3', artifact=datamake.artifacts.FileArtifact('/tmp/bar3')))

  def testParameterizedParameters(self):
    task_graph = self.dmconfig.task_graph()
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "command": "echo hello",
        "artifact": "/tmp/${filename}",
        "dependencies": ["task1"],
        "parameters": {
          "filename": "${category}-bar"
        }
      },
      {
        "id": "task2",
        "dependencies": ["task1"],
        "parameters": {
          "category": "foo"
        }
      } 
    ]
    """))
    task1, task2 = self.dmconfig.task_graph().resolve_subgraph('task2')
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo-bar')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2'))

  def testDoubleTaskParameterInheritence(self):
    task_graph = self.dmconfig.task_graph()
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "artifact": "/tmp/${category}/${action}/${label}"
      },
      {
        "id": "task2",
        "dependencies": ["task1"],
        "parameters": {
          "category": "foo"
        }
      },
      {
        "id": "task3",
        "dependencies": ["task1"],
        "parameters": {
          "action": "bar"
        }
      },
      {
        "id": "task4",
        "dependencies": ["task2", "task3"],
        "parameters": {
          "label": "baz"
        }
      }
    ]
    """))
    task1 = list(self.dmconfig.task_graph().resolve_subgraph('task4'))[0]
    self.assertEqual(task1, datamake.tasks.Task(id='task1', artifact=datamake.artifacts.FileArtifact('/tmp/foo/bar/baz')))

  def testDoubleTaskParameterMissingParam(self):
    task_graph = self.dmconfig.task_graph()
    self.dmconfig.load(StringIO("""[
      {
        "id": "task1",
        "artifact": "/tmp/${category}/${action}/${label}"
      },
      {
        "id": "task2",
        "dependencies": ["task1"],
        "parameters": {
          "category": "foo"
        }
      },
      {
        "id": "task3",
        "parameters": {
          "action": "bar"
        }
      },
      {
        "id": "task4",
        "dependencies": ["task2", "task3"],
        "parameters": {
          "label": "baz"
        }
      }
    ]
    """))
    self.assertRaises(datamake.tasks.TemplateKeyError, self.dmconfig.task_graph().resolve_subgraph, 'task4')

if __name__ == "__main__":
    unittest.main()

