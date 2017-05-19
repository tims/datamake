pipeline {
  agent any
  stages {
    stage('Test') {
      steps {
        sh 'virtualenv env'
        sh '''env/bin/pip install -r requirements.txt
'''
        sh 'env/bin/python setup.py test'
      }
    }
  }
}