import os, sys
import re
import requests
import urlparse
import parse_uri
import oursql
import urllib
import json
import httplib  # For crappy clone of part of webhdfs-py
from boto.s3.connection import S3Connection

def resolve_artifact(uri):
  if not uri:
    return None
    
  parsed_uri = parse_uri.ParseUri().parse(uri)
  if parsed_uri.protocol == "ssh":
    return SSHArtifact(parsed_uri.host, parsed_uri.path)
  elif parsed_uri.protocol == "http":
    return HTTPArtifact(parsed_uri.source)
  elif parsed_uri.protocol == "s3":
    return S3Artifact(parsed_uri.host, parsed_uri.path)
  elif parsed_uri.protocol == 'webhdfs':
    return HDFSArtifact(parsed_uri.host, parsed_uri.port, parsed_uri.user, parsed_uri.path)
  elif parsed_uri.protocol == "mysql":
    return MysqlArtifact(uri)
  elif parsed_uri.protocol == "test":
    return TestArtifact(parsed_uri.host)
  else:
    return FileArtifact(uri)

class Artifact:
  def uri(self):
    raise NotImplementedError()

  def exists(self):
    raise NotImplementedError("not implemented")

  def delete(self):
    raise NotImplementedError()

  def tuple(self): return (self.uri())
  def __eq__(self, other): return self.tuple() == other.tuple()
  def __ne__(self, other): return self.tuple() != other.tuple()
  def __lt__(self, other): return self.tuple() < other.tuple()
  def __le__(self, other): return self.tuple() <= other.tuple()
  def __gt__(self, other): return self.tuple() > other.tuple()
  def __ge__(self, other): return self.tuple() >= other.tuple()
  def __hash__(self): return hash(self.tuple())

class TestArtifact(Artifact):
  def __init__(self, exists_flag):
    self.exists_flag = exists_flag in ['true','True', True, 1]

  def uri(self):
    return "test://{0}".format(self.exists_flag)

  def exists(self):
    return self.exists_flag

  def delete(self):
    pass
    
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
    conn = S3Connection()
    b = conn.get_bucket(self.bucket)
    b.delete_key(self.key)

class HDFSArtifact(Artifact):
  # Until paul understands Python packaging better, we're going to have
  # to include the specific bit of code we want to use from
  # https://github.com/drelu/webhdfs-py/blob/master/webhdfs/webhdfs.py
  # here, which is the listdir method and its call graph.
  def __init__(self, namenode_host, namenode_port, hdfs_username, path):
    self.namenode_host = namenode_host
    self.namenode_port = namenode_port
    self.username = hdfs_username
    self.path = path

  def exists(self):
    url_path = '/webhdfs/v1' + self.path +'?op=LISTSTATUS&user.name=' + self.username
    httpClient = httplib.HTTPConnection(self.namenode_host,
                                        self.namenode_port,
                                        timeout=600)
    httpClient.request('GET', url_path, headers = {})
    response = httpClient.getresponse()
    data_dict = json.loads(response.read())
    if 'RemoteException' in data_dict:
      if 'exception' in data_dict['RemoteException']:
        if data_dict['RemoteException']['exception'] == 'FileNotFoundException':
          return False

    if 'FileStatuses' in data_dict:
      if 'FileStatus' in data_dict['FileStatuses']:
        return True

    return False

class MysqlArtifact(Artifact):
  def __init__(self, uri):
    uri_parser = parse_uri.ParseUri()
    parsed_uri = uri_parser.parse(uri)  
    params = {}
    if parsed_uri.host:
      params['host'] = parsed_uri.host
    if parsed_uri.user:
      params['user'] = parsed_uri.user
    if parsed_uri.password:
      params['passwd'] = urllib.unquote(parsed_uri.password)
    if parsed_uri.port:
      params['port'] = int(parsed_uri.port)
    database, query = parsed_uri.path.split('/')[1:]
    params['db'] = database
    self.connection_params = params
    self.query = urllib.unquote(query)
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
    raise NotImplementedError()

class SSHArtifact(Artifact):
  def __init__(self, host, path):
    self.host = host
    self.path = path

  def uri(self):
    return urlparse.urlunparse(['ssh',self.host,self.path,None,None,None])
 
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

