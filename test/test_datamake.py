import unittest
import json
from StringIO import StringIO
import datamake.tasks
import datamake.config
import datamake.artifacts

class DatamakeTestCase(unittest.TestCase):
  def setUp(self):
    self.dmconfig = datamake.config.DatamakeConfig()
    self.json_data = {
      "version": "1.0",
      "tasks": []
    }

  def testLoad(self):
    self.json_data['tasks'].append({"id": "task1"})
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    task_graph = self.dmconfig.task_graph()
    template_task = task_graph.task_templates['task1']
    self.assertEqual(template_task.id, 'task1', 'incorrect id')
    self.assertEqual(template_task.command, None, 'incorrect command')
    self.assertEqual(template_task.artifact, None, 'incorrect artifact')
    self.assertEqual(template_task.parameters, {}, 'incorrect parameter')


  def testSingleTask(self):
    self.json_data['tasks'].append({
            "id": "task1",
            "command": "echo hello world",
            "artifact": "/tmp/foo",
            "parameters": {
              "x": 1
            }
          } 
          )
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    task_graph = self.dmconfig.task_graph()
    template_task = task_graph.task_templates[u'task1']
    self.assertEqual(template_task.id, 'task1', 'incorrect id')
    self.assertEqual(template_task.command, 'echo hello world', 'incorrect command')
    self.assertEqual(template_task.artifact, '/tmp/foo', 'incorrect artifact')
    self.assertEqual(template_task.parameters, {'x': 1}, 'incorrect parameter')

    tasks = list(task_graph.get_tasks('task1'))
    self.assertEqual(len(tasks), 1)
    task = tasks[0]

    self.assertEqual(task.id, 'task1', 'incorrect id')
    self.assertEqual(task.command, 'echo hello world', 'incorrect command')
    self.assertEqual(task.artifact.uri(), '/tmp/foo', 'incorrect artifact')

  def testSingleParameterizedTask(self):
    self.json_data['tasks'].append({
            "id": "task1",
            "command": "echo ${message}",
            "artifact": "/tmp/${filename}",
            "parameters": {
              "message": "hello world",
              "filename": "foo"
            }
          })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    task_graph = self.dmconfig.task_graph()
    template_task = task_graph.task_templates['task1']
    self.assertEqual(template_task.id, 'task1', 'incorrect id')
    self.assertEqual(template_task.command, 'echo ${message}', 'incorrect command')
    self.assertEqual(template_task.artifact, '/tmp/${filename}', 'incorrect artifact')
    self.assertEqual(template_task.parameters, {'message':'hello world','filename':'foo'}, 'incorrect parameter')

    tasks = list(task_graph.get_tasks('task1'))
    self.assertEqual(len(tasks), 1)
    task = tasks[0]

    self.assertEqual(task.id, 'task1', 'incorrect id')
    self.assertEqual(task.command, 'echo hello world', 'incorrect command')
    self.assertEqual(task.artifact.uri(), '/tmp/foo', 'incorrect artifact')

  def testUpstreamTaskInheritsDownstreamParams(self):
    self.json_data['tasks'].append({
            "id": "task1",
            "command": "echo ${message}",
            "artifact": "/tmp/${filename}"
          })
    self.json_data['tasks'].append(
          {
            "id": "task2",
            "command": "echo goodbye",
            "artifact": "/tmp/bar",
            "dependencies": ["task1"],
            "parameters": {
              "message": "hello",
              "filename": "foo"
            }
          })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    task_graph = self.dmconfig.task_graph()
    task1, task2 = task_graph.get_tasks('task2')
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2', command='echo goodbye', artifact=datamake.artifacts.FileArtifact('/tmp/bar')))

  def testUpstreamParamsOverrideDownstreamParams(self):
    self.json_data['tasks'].append(
          {
            "id": "task1",
            "command": "echo ${message}",
            "artifact": "/tmp/${filename}",
            "parameters": {
              "message": "hello",
              "filename": "foo"
            }
          })
    self.json_data['tasks'].append(
          {
            "id": "task2",
            "command": "echo ${message}",
            "artifact": "/tmp/${filename}",
            "dependencies": ["task1"],
            "parameters": {
              "message": "goodbye",
              "filename": "bar"
            }
          })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    task_graph = self.dmconfig.task_graph()
    task1, task2 = task_graph.get_tasks('task2')
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2', command='echo goodbye', artifact=datamake.artifacts.FileArtifact('/tmp/bar')))

  def testEvalParams(self):
    self.json_data['tasks'].append(
        {
          "id": "task1",
          "command": "echo hello ${count}",
          "artifact": "/tmp/foo${count}",
          "parameters": {
            "count": "=1 + 1"
          }
        })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))

    task_graph = self.dmconfig.task_graph()
    task1, = task_graph.get_tasks('task1')
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello 2', artifact=datamake.artifacts.FileArtifact('/tmp/foo2')))

  def testEvalParamsToManyTasks(self):
    self.json_data['tasks'].append(
        {
          "id": "task1",
          "command": "echo hello ${count}",
          "artifact": "/tmp/foo${count}",
          "parameters": {
            "count": "=[1,2,3]"
          }
        })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    task_graph = self.dmconfig.task_graph()
    t1,t2,t3 = task_graph.get_tasks('task1')
    self.assertEqual(t1, datamake.tasks.Task(id='task1', command='echo hello 1', artifact=datamake.artifacts.FileArtifact('/tmp/foo1')))
    self.assertEqual(t2, datamake.tasks.Task(id='task1', command='echo hello 2', artifact=datamake.artifacts.FileArtifact('/tmp/foo2')))
    self.assertEqual(t3, datamake.tasks.Task(id='task1', command='echo hello 3', artifact=datamake.artifacts.FileArtifact('/tmp/foo3')))

  def testEvalParamsToManyDependentTasks(self):
    self.json_data['tasks'].append(
        {
          "id": "task1",
          "command": "echo hello ${count}",
          "artifact": "/tmp/foo${count}"
        })
    self.json_data['tasks'].append(
        {
          "id": "task2",
          "command": "echo goodbye",
          "artifact": "/tmp/bar",
          "dependencies": ["task1"],
          "parameters": {
            "count": "=[1,2,3]"
          }
        })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    task_graph = self.dmconfig.task_graph()
    t11,t12,t13,t2 = task_graph.get_tasks('task2')
    self.assertEqual(t11, datamake.tasks.Task(id='task1', command='echo hello 1', artifact=datamake.artifacts.FileArtifact('/tmp/foo1')))
    self.assertEqual(t12, datamake.tasks.Task(id='task1', command='echo hello 2', artifact=datamake.artifacts.FileArtifact('/tmp/foo2')))
    self.assertEqual(t13, datamake.tasks.Task(id='task1', command='echo hello 3', artifact=datamake.artifacts.FileArtifact('/tmp/foo3')))
    self.assertEqual(t2, datamake.tasks.Task(id='task2', command='echo goodbye', artifact=datamake.artifacts.FileArtifact('/tmp/bar')))

  def testEvalParamsToManyTasksWithSingleDependentTask(self):
    self.json_data['tasks'].append(
        {
          "id": "task1",
          "command": "echo hello",
          "artifact": "/tmp/foo"
        })
    self.json_data['tasks'].append(
        {
          "id": "task2",
          "command": "echo goodbye ${count}",
          "artifact": "/tmp/bar${count}",
          "dependencies": ["task1"],
          "parameters": {
            "count": "=[1,2,3]"
          }
        })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    execution_tasks = list(self.dmconfig.task_graph().get_tasks('task2'))
    t1,t21,t22,t23 = execution_tasks
    self.assertEqual(t1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo')))
    self.assertEqual(t21, datamake.tasks.Task(id='task2', command='echo goodbye 1', artifact=datamake.artifacts.FileArtifact('/tmp/bar1')))
    self.assertEqual(t22, datamake.tasks.Task(id='task2', command='echo goodbye 2', artifact=datamake.artifacts.FileArtifact('/tmp/bar2')))
    self.assertEqual(t23, datamake.tasks.Task(id='task2', command='echo goodbye 3', artifact=datamake.artifacts.FileArtifact('/tmp/bar3')))

  def testParameterizedParameters(self):
    self.json_data['tasks'].append(
        {
          "id": "task1",
          "command": "echo hello",
          "artifact": "/tmp/${filename}",
          "dependencies": ["task1"],
          "parameters": {
            "filename": "${category}-bar"
          }
        })
    self.json_data['tasks'].append(
        {
          "id": "task2",
          "dependencies": ["task1"],
          "parameters": {
            "category": "foo"
          }
        })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    task1, task2 = self.dmconfig.task_graph().get_tasks('task2')
    self.assertEqual(task1, datamake.tasks.Task(id='task1', command='echo hello', artifact=datamake.artifacts.FileArtifact('/tmp/foo-bar')))
    self.assertEqual(task2, datamake.tasks.Task(id='task2'))

  def testDoubleTaskParameterInheritence(self):
    self.json_data['tasks'].append(
        {
          "id": "task1",
          "artifact": "/tmp/${category}/${action}/${label}"
        })
    self.json_data['tasks'].append(
        {
          "id": "task2",
          "dependencies": ["task1"],
          "parameters": {
            "category": "foo"
          }
        })
    self.json_data['tasks'].append(
        {
          "id": "task3",
          "dependencies": ["task1"],
          "parameters": {
            "action": "bar"
          }
        })
    self.json_data['tasks'].append(
        {
          "id": "task4",
          "dependencies": ["task2", "task3"],
          "parameters": {
            "label": "baz"
          }
        })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    tasks = list(self.dmconfig.task_graph().get_tasks('task4'))
    task1 = list(tasks)[0]
    self.assertEquals(len(tasks), 4)
    self.assertEqual(task1, datamake.tasks.Task(id='task1', artifact=datamake.artifacts.FileArtifact('/tmp/foo/bar/baz')))

  def testDoubleTaskParameterMissingParam(self):
    self.json_data['tasks'].append(
        {
          "id": "task1",
          "artifact": "/tmp/${category}/${action}/${label}"
        })
    self.json_data['tasks'].append(
        {
          "id": "task2",
          "dependencies": ["task1"],
          "parameters": {
            "category": "foo"
          }
        })
    self.json_data['tasks'].append(
        {
          "id": "task3",
          "parameters": {
            "action": "bar"
          }
        })
    self.json_data['tasks'].append(
        {
          "id": "task4",
          "dependencies": ["task2", "task3"],
          "parameters": {
            "label": "baz"
          }
        })
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    self.assertRaises(datamake.tasks.TemplateKeyError, self.dmconfig.task_graph().get_tasks, 'task4')


  def testTaskDoesNotExist(self):
    self.dmconfig.load(StringIO(json.dumps(self.json_data)))
    self.assertRaises(datamake.tasks.TaskNotFoundError, self.dmconfig.task_graph().get_tasks, 'task_id')



if __name__ == "__main__":
    unittest.main()

