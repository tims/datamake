pipeline {
  agent any
  stages {
    stage('Test') {
      steps {
        sh 'pip install -r requirements.txt'
        sh 'python setup.py test'
      }
    }
  }
}