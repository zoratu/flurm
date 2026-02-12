pipeline {
  agent any

  options {
    timestamps()
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '30'))
  }

  environment {
    FLURM_CHECK_DOCKER = '0'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Quick Checks') {
      steps {
        sh 'make check-quick'
      }
    }

    stage('Coverage Gate') {
      steps {
        sh 'make check-prepush'
      }
    }
  }

  post {
    always {
      cleanWs()
    }
  }
}
