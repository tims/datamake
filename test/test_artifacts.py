import unittest
import json
from StringIO import StringIO
import datamake.tasks
import datamake.config
import datamake.artifacts

class DatamakeConfigTest(unittest.TestCase):
  def testResolveFileArtifact(self):
    art = datamake.artifacts.resolve_artifact('/tmp/file')
    self.assertEquals(art.__class__, datamake.artifacts.FileArtifact)
    self.assertEquals(art.uri(), '/tmp/file')

  def testResolveMysqlArtifact(self):
    uri = "mysql://user:password@example.com:3306/mydatabase/select * from users limit 1"
    conn_params = {
      "db": "mydatabase",
      "host": "example.com",
      "user": "user",
      "passwd": "password",
      "port": 3306
    }
    query = "select * from users limit 1"
    art = datamake.artifacts.resolve_artifact(uri)
    self.assertEquals(art.__class__, datamake.artifacts.MysqlArtifact)
    self.assertEquals(art.connection_params, conn_params)
    self.assertEquals(art.query, query)
    self.assertEquals(art.uri(), uri)
  
  def testResolveMysqlArtifactPercentEncoding(self):
    uri = "mysql://user:pass%20word@example.com:3306/mydatabase/select%20%2A%20from%20users%20limit%201"
    conn_params = {
      "db": "mydatabase",
      "host": "example.com",
      "user": "user",
      "passwd": "pass word",
      "port": 3306
    }
    query = "select * from users limit 1"
    art = datamake.artifacts.resolve_artifact(uri)
    self.assertEquals(art.__class__, datamake.artifacts.MysqlArtifact)
    self.assertEquals(art.connection_params, conn_params)
    self.assertEquals(art.query, query)
    self.assertEquals(art.uri(), uri)

  def testResolveS3Artifact(self):
    uri = 's3://bucket/this/is/a/key'
    art = datamake.artifacts.resolve_artifact(uri)
    self.assertEquals(art.__class__, datamake.artifacts.S3Artifact)
    self.assertEquals(art.bucket, "bucket")
    self.assertEquals(art.key, "/this/is/a/key")
    self.assertEquals(art.uri(), uri)

  def testResolveHttpArtifact(self):
    uri = 'http://example.com/path'
    art = datamake.artifacts.resolve_artifact(uri)
    self.assertEquals(art.__class__, datamake.artifacts.HTTPArtifact)
    self.assertEquals(art.uri(), uri)

  def testResolveSSHArtifact(self):
    uri = 'ssh://example.com/this/is/a/path'
    art = datamake.artifacts.resolve_artifact(uri)
    self.assertEquals(art.__class__, datamake.artifacts.SSHArtifact)
    self.assertEquals(art.host, "example.com")
    self.assertEquals(art.path, "/this/is/a/path")
    self.assertEquals(art.uri(), uri)

if __name__ == "__main__":
    unittest.main()

