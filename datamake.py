import sys
import os
import json
import requests
from parse_uri import ParseUri
from string import Template
import collections
import itertools
import datetime

class Artifact:
  def exists(self):
    raise Exception("not implemented")

  def delete(self):
    raise Exception("not implemented")
  
class SSHArtifact(Artifact):
  def __init__(self, host, path):
    self.host = host
    self.path = path

  def exists(self):
    # TODO: This should throw exceptions on any errors and only return False when we genuinely know the file is not there
    command = 'ssh %s "[ -f %s ]"' % (self.host, self.path)
    if not os.system(command):
      # file exists
      return True
    return False

  def delete(self):
    command = 'ssh %s "rm %s"' % (self.host, self.path)
    os.system(command)

class FileArtifact(Artifact):
  def __init__(self, path):
    self.path = path

  def exists(self):
    if not self.path:
      raise Exception("invalid path " + self.path)
    command = '[ -f %s ]' % self.path
    if not os.system(command):
      return True
    return False

  def delete(self):
    command = 'rm %s' % self.path
    print command
    os.system(command)

def resolve_artifact(uri):
  uri_parser = ParseUri()
  parsed_uri = uri_parser.parse(uri)  
  if parsed_uri.protocol == "ssh":
    return SSHArtifact(parsed_uri.host, parsed_uri.path)
  elif parsed_uri.protocol == "file":
    return FileArtifact(parsed_uri.path)
  else:
    return Artifact()

def resolve_date(year,month,day,day_delta=0):
  dt = datetime.datetime(year,month,day) - datetime.timedelta(days=day_delta)
  return dt - delta

class Job:
  def __init__(self, jobid, parameters={}):
    self.parameters = parameters

    jobs_dir = 'jobs'
    job_filename = os.path.join(jobs_dir, jobid + ".job")
    job_file = open(job_filename)
    jobconf = json.load(job_file)

    self.jobid = jobid
    self.command = Template(jobconf["command"]).substitute(self.parameters)
    self.artifact = resolve_artifact(Template(jobconf["artifact"]).substitute(self.parameters))

    self.dependencies = []
    for dependency_conf in jobconf.get("dependencies", []):
      for params in self.resolve_dependency_parameters(dependency_conf['parameters']):
        self.dependencies.append(Job(dependency_conf['jobid'], params))

  def resolve_dependency_parameters(self, dependency_parameters):
    print jobid, "resolving parameters"
    templated_params = {}
    for key, values in dependency_parameters.items():
      for value in values:
        try:
          value = Template(value).substitute(self.parameters)
        except KeyError, e:
          print "Could not resolve template parameter", e
          raise
        templated_params[key] = templated_params.get(key, []) + [value]

    for point in itertools.product(*templated_params.values()):
      params = dict(zip(templated_params.iterkeys(), point))
      yield params

  def run(self):
    print self.jobid, "command:", self.command
    ret = os.system(self.command)
    if ret:
      print "Error",ret
      sys.exit(ret)

  def build(self):
    if self.artifact.exists():
      print self.jobid, "artifact present, nothing to do", sorted(self.artifact.__dict__.items())
    else:
      print self.jobid, "artifact not present", sorted(self.artifact.__dict__.items())
      print self.jobid, "checking dependencies"
      for dependency in self.dependencies:
        dependency.build()
    
      print self.jobid, "Starting with parameters", self.parameters
      self.run()
      print self.jobid, "finished"
  
jobid = sys.argv[1]
Job(jobid, {}).build()

