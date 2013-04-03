import unittest
import json
from StringIO import StringIO
import datamake.tasks
import datamake.config
import datamake.artifacts
import datamake.datamake

class DatamakeTestCase(unittest.TestCase):
  def get_template_resolver(self, task_infos):
    config = datamake.config.DatamakeConfig()
    self.json_data = {
      "version": "1.0",
      "tasks": task_infos
    }
    config.load(StringIO(json.dumps(self.json_data)))
    return datamake.datamake.get_template_resolver(config)

  def testLoad(self):
    template_resolver = self.get_template_resolver([{"id": "task1"}])
    template_task = template_resolver.templates['task1']
    self.assertEqual(template_task.id, 'task1', 'incorrect id')
    self.assertEqual(template_task.command, None, 'incorrect command')
    self.assertEqual(template_task.artifact, None, 'incorrect artifact')
    self.assertEqual(template_task.parameters, {}, 'incorrect parameter')


  def testSingleTask(self):
    template_resolver = self.get_template_resolver([
      {
        "id": "task1",
        "command": "echo hello world",
        "artifact": "/tmp/foo",
        "parameters": {
          "x": 1
        }
      } 
    ])
    template_task = template_resolver.templates['task1']
    self.assertEqual(template_task.id, 'task1', 'incorrect id')
    self.assertEqual(template_task.command, 'echo hello world', 'incorrect command')
    self.assertEqual(template_task.artifact, '/tmp/foo', 'incorrect artifact')
    self.assertEqual(template_task.parameters, {'x': 1}, 'incorrect parameter')
    task_graph = template_resolver.resolve_task_graph('task1')

    self.assertEquals(1, len(task_graph.nodes()))
    task = task_graph.node['task1']['task']
    self.assertEqual(task.id, 'task1', 'incorrect id')
    self.assertEqual(task.command, 'echo hello world', 'incorrect command')
    self.assertEqual(task.artifact.uri(), '/tmp/foo', 'incorrect artifact')

  def testSingleParameterizedTask(self):
    template_resolver = self.get_template_resolver([
      {
        "id": "task1",
        "command": "echo ${message}",
        "artifact": "/tmp/${filename}",
        "parameters": {
          "message": "hello world",
          "filename": "foo"
        }
      }
    ])
    template_task = template_resolver.templates['task1']
    self.assertEqual(template_task.id, 'task1', 'incorrect id')
    self.assertEqual(template_task.command, 'echo ${message}', 'incorrect command')
    self.assertEqual(template_task.artifact, '/tmp/${filename}', 'incorrect artifact')
    self.assertEqual(template_task.parameters, {'message':'hello world','filename':'foo'}, 'incorrect parameter')

    task_graph = template_resolver.resolve_task_graph('task1')
    task = task_graph.node['task1']['task']
    self.assertEqual(task.id, 'task1', 'incorrect id')
    self.assertEqual(task.command, 'echo hello world', 'incorrect command')
    self.assertEqual(task.artifact.uri(), '/tmp/foo', 'incorrect artifact')

  def testUpstreamTaskInheritsDownstreamParams(self):
    template_resolver = self.get_template_resolver([
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
    ])
    task_graph = template_resolver.resolve_task_graph('task2')
    task1, task2 = task_graph.node['task1']['task'], task_graph.node['task2']['task']
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2', command='echo goodbye', artifact=datamake.artifacts.FileArtifact('/tmp/bar')))

  def testUpstreamParamsOverrideDownstreamParams(self):
    template_resolver = self.get_template_resolver([
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
    ])    
    task_graph = template_resolver.resolve_task_graph('task2')
    task1, task2 = task_graph.node['task1']['task'], task_graph.node['task2']['task']
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2', command='echo goodbye', artifact=datamake.artifacts.FileArtifact('/tmp/bar')))

  def testParameterizedParameters(self):
    template_resolver = self.get_template_resolver([
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
    ])
    task_graph = template_resolver.resolve_task_graph('task2')
    task1, task2 = task_graph.node['task1']['task'], task_graph.node['task2']['task']
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo-bar')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2'))

  def testDiamondShapedParameterInheritence(self):
    template_resolver = self.get_template_resolver([
      {
        "id": "task1",
        "artifact": "/${A}/${B}/${C}/${D}",
        "parameters": {
          "A": "foe"
        }
      },
      {
        "id": "task2",
        "artifact": "/${B}/${D}",
        "dependencies": ["task1"],
        "parameters": {
          "B": "foo"
        }
      },
      {
        "id": "task3",
        "artifact": "/${C}/${D}",
        "dependencies": ["task1"],
        "parameters": {
          "C": "bar"
        }
      },
      {
        "id": "task4",
        "artifact": "/${D}",
        "dependencies": ["task2", "task3"],
        "parameters": {
          "D": "baz"
        }
      }
    ])
    task_graph = template_resolver.resolve_task_graph('task4')
    
    task1 = task_graph.node['task1']['task']
    task2 = task_graph.node['task2']['task']
    task3 = task_graph.node['task3']['task']
    task4 = task_graph.node['task4']['task']

    self.assertEqual(task1, datamake.tasks.Task(id='task1', artifact=datamake.artifacts.FileArtifact('/foe/foo/bar/baz')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2', artifact=datamake.artifacts.FileArtifact('/foo/baz')))
    self.assertEqual(task3, datamake.tasks.Task(id='task3', artifact=datamake.artifacts.FileArtifact('/bar/baz')))
    self.assertEqual(task4, datamake.tasks.Task(id='task4', artifact=datamake.artifacts.FileArtifact('/baz')))

  def testDoubleTaskParameterMissingParam(self):
    template_resolver = self.get_template_resolver([
      {
        "id": "task1",
        "artifact": "/${A}/${B}/${C}/${D}",
        "parameters": {
          "A": "foe"
        }
      },
      {
        "id": "task2",
        "artifact": "/${B}/${D}",
        "dependencies": ["task1"],
        "parameters": {
          "B": "foo"
        }
      },
      {
        "id": "task3",
        "artifact": "/${C}/${D}",
        "parameters": {
          "C": "bar"
        }
      },
      {
        "id": "task4",
        "artifact": "/${D}",
        "dependencies": ["task2", "task3"],
        "parameters": {
          "D": "baz"
        }
      }
    ])
    self.assertRaises(datamake.templates.TemplateKeyError, template_resolver.resolve_task_graph, 'task4')

  def testTaskDoesNotExist(self):
    template_resolver = self.get_template_resolver([])
    self.assertRaises(KeyError, template_resolver.resolve_task_graph, 'task1')



if __name__ == "__main__":
    unittest.main()

