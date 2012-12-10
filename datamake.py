import sys, os, datetime
try:
    import json
except ImportError:
    import simplejson as json
from string import Template
import requests
from parse_uri import ParseUri
from boto.s3.connection import S3Connection
import urllib
import subprocess
import oursql

def date_now():  
  dt = datetime.datetime.now()
  return dt.strftime("%Y-%m-%d")

def date_days_delta(datestring, days_delta):
  dt = datetime.datetime.strptime(datestring, "%Y-%m-%d")
  delta = datetime.timedelta(int(days_delta))
  return (dt + delta).strftime("%Y-%m-%d")

class Artifact:
  def uri(self):
    raise Exception("not implemented")

  def exists(self):
    raise Exception("not implemented")

  def delete(self):
    raise Exception("not implemented")

class S3Artifact(Artifact):
  def __init__(self, bucket, key):
    self.bucket = bucket
    self.key = key

  def uri(self):
    return "s3://%s%s" % (self.bucket, self.key)

  def exists(self):
    from boto.s3.connection import S3Connection
    conn = S3Connection()
    b = conn.get_bucket(self.bucket)
    if b.get_key(self.key):
      return True
    else:
      return False

  def delete(self):
    from boto.s3.connection import S3Connection
    conn = S3Connection(self.access_id, self.private_key)
    b = conn.get_bucket(self.bucket)
    b.delete_key(self.key)

class HTTPArtifact(Artifact):
  def __init__(self, url):
    self.url = url

  def uri(self):
    return self.url

  def exists(self):
    r = requests.head(self.url)
    if r.status_code == 404:
      return False
    elif r.status_code == 200 or r.status_code == 302:
      return True
    else:
      raise Exception("Unexpected status code: %s" % r.status_code)

  def delete(self):
    r = requests.delete(self.url)


class MysqlArtifact(Artifact):
  def __init__(self, uri):
    uri_parser = ParseUri()
    parsed_uri = uri_parser.parse(uri)  
    params = {}
    if parsed_uri.host:
      params['host'] = parsed_uri.host
    if parsed_uri.user:
      params['user'] = parsed_uri.user
    if parsed_uri.password:
      params['passwd'] = parsed_uri.password
    if parsed_uri.port:
      params['port'] = parsed_uri.port
    database, query = parsed_uri.path.split('/')[1:]
    params['db'] = database
    self.connection_params = params
    self.query = query
    self.parsed_uri = parsed_uri

  def uri(self):
    return self.parsed_uri.source

  def exists(self):
    conn = oursql.connect(**self.connection_params)
    curs = conn.cursor()
    print "Executing query", self.query
    curs.execute(self.query)
    rows = curs.fetchall()
    print rows
    if len(rows) > 0:
      return True
    else:
      return False

  def delete(self):
    raise Exception("not implemented")

class SSHArtifact(Artifact):
  def __init__(self, host, path):
    self.host = host
    self.path = path

  def uri(self):
    return "ssh://%s/%s" % (self.host, self.path)
 
  def exists(self):
    # TODO: This should throw exceptions on any errors and only return False when we genuinely know the file is not there
    command = 'ssh %s "[ -f %s ]"' % (self.host, self.path)
    if not os.system(command):
      # file exists
      return True
    else:
      return False

  def delete(self):
    command = 'ssh %s "rm %s"' % (self.host, self.path)
    os.system(command)

class FileArtifact(Artifact):
  def __init__(self, path):
    self.path = path

  def uri(self):
    return self.path

  def exists(self):
    if not self.path:
      raise Exception("invalid path " + self.path)
    command = '[ -f %s ]' % self.path
    print "exists command:", command
    if not os.system(command):
      return True
    else:
      return False

  def delete(self):
    command = 'rm %s' % self.path
    os.system(command)

def resolve_artifact(uri):
  uri_parser = ParseUri()
  parsed_uri = uri_parser.parse(uri)  
  if parsed_uri.protocol == "ssh":
    return SSHArtifact(parsed_uri.host, parsed_uri.path)
  elif parsed_uri.protocol == "http":
    return HTTPArtifact(parsed_uri.source)
  elif parsed_uri.protocol == "s3":
    return S3Artifact(parsed_uri.host, parsed_uri.path)
  elif parsed_uri.protocol == "mysql":
    return MysqlArtifact(uri)
  else:
    return FileArtifact(uri)

def resolve_date(year,month,day,day_delta=0):
  dt = datetime.datetime(year,month,day) - datetime.timedelta(days=day_delta)
  return dt - delta

class JobFactory:
  def __init__(self, jobs_filename):
    self.jobs_filename = jobs_filename
    jobs_file = open(self.jobs_filename)
    self.conf = json.load(jobs_file)
    jobs_file.close()

  def get_job(self, job_id, parameters={}):
    for jobconf in self.conf:
      if jobconf['id'] == job_id:
        jobid = jobconf['id']
        
        job_parameters = jobconf.get('parameters',{})
        for k,v in job_parameters.items():
          job_parameters[k] = Template(v).substitute(parameters)
        job_parameters.update(dict(parameters))

        command = Template(jobconf["command"]).substitute(job_parameters) if "command" in jobconf else None
        artifact = resolve_artifact(Template(jobconf["artifact"]).substitute(job_parameters)) if "artifact" in jobconf else None
        is_artifact_temporary = jobconf.get('is_artifact_temporary', False)
        if not (is_artifact_temporary == True or is_artifact_temporary == False):
          raise "Job %s has invalid value %s for is_artifact_temporary" % (jobid, is_artifact_temporary)
        dependencies = []
        for dependency_conf in jobconf.get("dependencies", []):
          print jobid, "resolving parameters"
          for params in self.resolve_dependency_parameters(dependency_conf.get('parameters',{}), job_parameters):
            job = self.get_job(dependency_conf['id'], params)
            dependencies.append(job)
        return Job(jobid=jobid, command=command, artifact=artifact, dependencies=dependencies, is_artifact_temporary=is_artifact_temporary)
    raise Exception("Job id not found %s" % job_id)

  def resolve_dependency_parameters(self, dependency_parameters, inherited_parameters):
    templated_params = {}
    for key, value in dependency_parameters.items():
      try:
        value = Template(value).substitute(inherited_parameters)
      except KeyError, e:
        print "Could not resolve template parameter", e
        raise
      if not value.startswith("="):
        templated_params[key] = templated_params.get(key, []) + [value]
      else:
        eval_value = eval(value[1:])
        if isinstance(eval_value, basestring):
          templated_params[key] = templated_params.get(key, []) + [eval_value]
        elif hasattr(eval_value, "__iter__"):
          for v in eval_value:
            templated_params[key] = templated_params.get(key, []) + [v]
        else:
          raise "Unable to handle evalutation of parameter, must be string or iterable: %s = %s" % (key, eval_value)

    for point in product(*templated_params.values()):
      params = dict(zip(templated_params.iterkeys(), point))
      for k,v in inherited_parameters.iteritems():
        if k not in params: params[k] = v
      yield params

job_factory = None

# itertools.product for python 2.5
def product(*args, **kwds):
    # product('ABCD', 'xy') --> Ax Ay Bx By Cx Cy Dx Dy
    # product(range(2), repeat=3) --> 000 001 010 011 100 101 110 111
    pools = map(tuple, args) * kwds.get('repeat', 1)
    result = [[]]
    for pool in pools:
        result = [x+[y] for x in result for y in pool]
    for prod in result:
        yield tuple(prod)

class Job:
  def __init__(self, jobid, command, artifact, dependencies=[], is_artifact_temporary=False):
    self.jobid = jobid
    self.command = command
    self.artifact = artifact
    self.dependencies = dependencies
    self.is_artifact_temporary = is_artifact_temporary
    

  def run(self):
    print self.jobid, "command:", self.command
    subprocess.check_call(self.command, shell=True)

  def cleanup(self):
    if self.artifact and self.is_artifact_temporary:
      print self.jobid, "cleaning up", self.artifact.uri()
      self.artifact.delete()

  def build(self):
    if self.artifact:
      if self.artifact.exists():
        print self.jobid, "artifact present, nothing to do.", self.artifact.uri()
        return
      else:
        print self.jobid, "artifact not present.", self.artifact.uri()

    print self.jobid, "checking dependencies"
    for dependency in self.dependencies:
      dependency.build()

    if self.command:
      print self.jobid, "Starting"
      self.run()
      print self.jobid, "finished"

    for dependency in self.dependencies:
      dependency.cleanup()


  
def main(args):
  job_file = args[1]
  job_id = args[2]
  params = {}
  for arg in args[3:]:
    params.update([arg.split("=")])
  job_factory = JobFactory(job_file)
  job = job_factory.get_job(job_id, params)
  job.build()

if __name__ == "__main__":
  main(sys.argv)

