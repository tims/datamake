import os, sys
import re
import requests
import parse_uri
import oursql
from boto.s3.connection import S3Connection

def resolve_artifact(uri):
  parsed_uri = parse_uri.ParseUri().parse(uri)
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

class Artifact:
  def uri(self):
    raise Exception("not implemented")

  def exists(self):
    raise Exception("not implemented")

  def delete(self):
    raise Exception("not implemented")

class FileArtifact(Artifact):
  def __init__(self, path):
    self.path = path

  def uri(self):
    return self.path

  def exists(self):
    if not self.path:
      raise Exception("invalid path " + self.path)
    command = '[ -f %s ]' % self.path
    if not os.system(command):
      return True
    else:
      return False

  def delete(self):
    command = 'rm %s' % self.path
    os.system(command)

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
    curs.execute(self.query)
    rows = curs.fetchall()
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

