import sys
import json
import networkx
import itertools
import subprocess
from string import Template
import artifacts
from utils import *

class TaskExecutionError(Exception):
  def __init__(self, task, message=None):
    self.task = task
    self.message = message

  def __str__(self):
    return repr((self.task.__dict__, message))

class Task:
  def __init__(self, **kvargs):
    self.id = kvargs['id']
    self.pre_command = None
    self.post_command = None
    self.command = kvargs.get('command', None)
    self.artifact = kvargs.get('artifact', None)
    self.cleanup = kvargs.get('cleanup', False)
    self.max_attempts = kvargs.get('max_attempts', 1)
    self.template = kvargs.get('template', None)

  def _run_command(self):
    try:
      command = self.command
      if self.pre_command:
        command = "{0} && {1}".format(self.pre_command, command)
      if self.post_command:
        command = "{0} && {1}".format(command, self.post_command)
      print self.command
      subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError:
      raise TaskExecutionError(self)
    finally:
      sys.stdout.flush()
      sys.stderr.flush()

  def execute(self):
    attempts = 0
    while True:
      try:
        if self.command:
          attempts += 1
          self._run_command()
          return
      except TaskExecutionError:
        print "attempt {0} failed".format(attempts)
        if attempts >= self.max_attempts:
          print "max attempts reached"
          raise

  def delete_artifact(self, force=False):
    if self.artifact:
      if self.cleanup or force:
        if self.artifact.exists():
          print "Deleting artifact", self.artifact.uri()
          try:
            self.artifact.delete()
          except NotImplementedError:
            print "Cannot delete artifact {0}".format(artifact.uri())

  def tuple(self): return (self.id, self.command, self.artifact.uri() if self.artifact else None, self.cleanup, self.max_attempts)
  def __eq__(self, other): return self.tuple() == other.tuple()
  def __ne__(self, other): return self.tuple() != other.tuple()
  def __lt__(self, other): return self.tuple() < other.tuple()
  def __le__(self, other): return self.tuple() <= other.tuple()
  def __gt__(self, other): return self.tuple() > other.tuple()
  def __ge__(self, other): return self.tuple() >= other.tuple()
  def __hash__(self): return hash(self.tuple())



